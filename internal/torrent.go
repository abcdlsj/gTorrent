package internal

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jackpal/bencode-go"
)

type TorrentMeta struct {
	Announce string `bencode:"announce"`
	Info     info   `bencode:"info"`
}

type info struct {
	PieceLength int         `bencode:"piece length"`
	Length      int         `bencode:"length"`
	Pieces      string      `bencode:"pieces"`
	Name        string      `bencode:"name"`
	Files       []multiFile `bencode:"files,omitempty"`
}

type multiFile struct {
	Length int      `bencode:"length"`
	Path   []string `bencode:"path"`
}

type Torrent struct {
	Name     string
	Port     int
	Length   int
	Announce string
	InfoHash [20]byte
	PeerID   [20]byte

	Peers        []Peer
	pieces       [][20]byte
	piecesLength int
}

type TrackerResponse struct {
	FailureReason string `bencode:"failure reason,omitempty"`
	Interval      int    `bencode:"interval,omitempty"`
	Peers         string `bencode:"peers,omitempty"`
}

type PeerClient struct {
	conn     net.Conn
	bf       Bitfields
	IP       net.IP
	Port     uint16
	Choked   bool
	InfoHash [20]byte
	PeerID   [20]byte

	index int
}

type Peer struct {
	IP   net.IP
	Port uint16
}

type MessageID uint8

const (
	MsgChoke         MessageID = 0
	MsgUnChoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
)

type Message struct {
	ID      MessageID
	Payload []byte
}

func Open(path string) (*TorrentMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var t TorrentMeta
	if err := bencode.Unmarshal(file, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

func (t *Torrent) Detail() {
	log.Printf("announce: %s\n", t.Announce)
	log.Printf("self_peer_id: %x\n", t.PeerID)
	log.Printf("length: %d\n", t.Length)
	log.Printf("pieces_length: %d\n", t.piecesLength)
}

func (t *TorrentMeta) Torrent() *Torrent {
	var peerID [20]byte
	_, err := rand.Read(peerID[:])
	if err != nil {
		return nil
	}

	tt := &Torrent{
		Announce: t.Announce,
		PeerID:   peerID,
		Port:     6881,
		InfoHash: sha1.Sum(bencodeStruct(t.Info)),

		Name: t.Info.Name,

		pieces:       t.Info.splitPiecesHashes(),
		piecesLength: t.Info.PieceLength,
	}

	if len(t.Info.Files) == 0 {
		tt.Length = t.Info.Length
	} else {
		for _, file := range t.Info.Files {
			tt.Length += file.Length
		}
	}

	return tt
}

func (i *info) splitPiecesHashes() [][20]byte {
	hashLen := 20
	buf := []byte(i.Pieces)

	numHashes := len(buf) / hashLen
	hashes := make([][20]byte, numHashes)

	for i := 0; i < numHashes; i++ {
		copy(hashes[i][:], buf[i*hashLen:(i+1)*hashLen])
	}

	return hashes
}

func (t *Torrent) buildTracker() string {
	base, err := url.Parse(t.Announce)
	if err != nil {
		panic(err)
	}
	q := base.Query()
	q.Set("info_hash", string(t.InfoHash[:]))
	q.Set("peer_id", string(t.PeerID[:]))
	q.Set("port", strconv.Itoa(t.Port))
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")
	q.Set("compact", "1")
	q.Set("left", strconv.Itoa(t.Length))
	base.RawQuery = q.Encode()

	return base.String()
}

func (t *Torrent) RequestTracker() {
	c := &http.Client{Timeout: 15 * time.Second}
	resp, err := c.Get(t.buildTracker())
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var trackerResp TrackerResponse
	if err = bencode.Unmarshal(resp.Body, &trackerResp); err != nil {
		panic(err)
	}
	if trackerResp.FailureReason != "" {
		panic(trackerResp.FailureReason)
	}
	t.Peers = parsePeer([]byte(trackerResp.Peers))
}

func (t *Torrent) calculateIndexBeginAndEnd(index int) (int, int) {
	begin := index * t.piecesLength
	end := begin + t.piecesLength
	if end > t.Length {
		end = t.Length
	}
	return begin, end
}

func (t *Torrent) calculateLength(index int) int {
	begin, end := t.calculateIndexBeginAndEnd(index)
	return end - begin
}

func (t *Torrent) Download() []byte {
	workChan := make(chan pieceWork, len(t.pieces))
	resultChan := make(chan pieceResult)

	for index, hash := range t.pieces {
		workChan <- pieceWork{
			index:  index,
			hash:   hash,
			length: t.calculateLength(index),
		}
	}

	i := 0
	for _, p := range t.Peers {
		if p.IP == nil {
			continue
		}
		peerClient := PeerClient{
			IP:       p.IP,
			Port:     p.Port,
			InfoHash: t.InfoHash,
			PeerID:   t.PeerID,
			index:    i,
		}
		i++
		go peerClient.download(workChan, resultChan)
	}

	// Collect results into a buffer until full
	buf := make([]byte, t.Length)
	donePieces := 0
	for donePieces < len(t.pieces) {
		res := <-resultChan
		begin, end := t.calculateIndexBeginAndEnd(res.index)
		copy(buf[begin:end], res.data)
		donePieces++
		percent := float64(donePieces) / float64(len(t.pieces)) * 100
		log.Printf("(%0.2f%%) Downloaded piece #%d from no.%d peer\n", percent, res.index, res.peerIndex)
	}
	close(workChan)
	return buf
}

func (p *PeerClient) download(workChan chan pieceWork, result chan pieceResult) {
	err := p.handshake()
	if err != nil {
		log.Printf("handshake error, %s:%d, %s\n", p.IP.String(), p.Port, err)
		return
	}
	p.recvBitfield()

	p.sendUnchoke()
	p.sendInterested()

	for work := range workChan {
		if !p.bf.HasPiece(work.index) {
			workChan <- work
			continue
		}

		// Download the piece
		buf, err := attemptDownloadPiece(p, work)
		if err != nil {
			log.Println("Exiting", err)
			workChan <- work
			return
		}

		err = checkIntegrity(work, buf)
		if err != nil {
			log.Printf("Piece #%d failed integrity check\n", work.index)
			workChan <- work // Put piece back on the queue
			continue
		}

		p.SendHave(work.index)
		result <- pieceResult{work.index, buf, p.index}
	}
}

type pieceProgress struct {
	index  int
	client *PeerClient
	buf    []byte

	downloaded int
	requested  int
	backlog    int
}

// MaxBlockSize is the largest number of bytes a request can ask for
const MaxBlockSize = 16384

// MaxBacklog is the number of unfulfilled requests a client can have in its pipeline
const MaxBacklog = 5

func attemptDownloadPiece(p *PeerClient, pw pieceWork) ([]byte, error) {
	state := pieceProgress{
		index:  pw.index,
		client: p,
		buf:    make([]byte, pw.length),
	}

	// Setting a deadline helps get unresponsive peers unstuck.
	// 30 seconds is more than enough time to download a 262 KB piece
	p.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer p.conn.SetDeadline(time.Time{}) // Disable the deadline

	for state.downloaded < pw.length {
		// If unchoked, send requests until we have enough unfulfilled requests
		if !state.client.Choked {
			for state.backlog < MaxBacklog && state.requested < pw.length {
				blockSize := MaxBlockSize
				// Last block might be shorter than the typical block
				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}

				err := p.SendRequest(pw.index, state.requested, blockSize)
				if err != nil {
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
		}

		err := state.readMessage()
		if err != nil {
			return nil, err
		}
	}

	return state.buf, nil
}

func (state *pieceProgress) readMessage() error {
	msg, err := ReadMessage(state.client.conn) // this call blocks
	if err != nil {
		return err
	}

	if msg == nil { // keep-alive
		return nil
	}

	switch msg.ID {
	case MsgUnChoke:
		state.client.Choked = false
	case MsgChoke:
		state.client.Choked = true
	case MsgHave:
		index, err := ParseHave(msg)
		if err != nil {
			return err
		}
		state.client.bf.SetPiece(index)
	case MsgPiece:
		n, err := ParsePiece(state.index, state.buf, msg)
		if err != nil {
			return err
		}
		state.downloaded += n
		state.backlog--
	}
	return nil
}

func ParseHave(msg *Message) (int, error) {
	if msg.ID != MsgHave {
		return 0, fmt.Errorf("expected HAVE (ID %d), got ID %d", MsgHave, msg.ID)
	}
	if len(msg.Payload) != 4 {
		return 0, fmt.Errorf("expected payload length 4, got length %d", len(msg.Payload))
	}
	index := int(binary.BigEndian.Uint32(msg.Payload))
	return index, nil
}

// ParsePiece parses a PIECE message and copies its payload into a buffer
func ParsePiece(index int, buf []byte, msg *Message) (int, error) {
	if msg.ID != MsgPiece {
		return 0, fmt.Errorf("expected PIECE (ID %d), got ID %d", MsgPiece, msg.ID)
	}
	if len(msg.Payload) < 8 {
		return 0, fmt.Errorf("payload too short. %d < 8", len(msg.Payload))
	}
	parsedIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	if parsedIndex != index {
		return 0, fmt.Errorf("expected index %d, got %d", index, parsedIndex)
	}
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	if begin >= len(buf) {
		return 0, fmt.Errorf("begin offset too high. %d >= %d", begin, len(buf))
	}
	data := msg.Payload[8:]
	if begin+len(data) > len(buf) {
		return 0, fmt.Errorf("data too long [%d] for offset %d with length %d", len(data), begin, len(buf))
	}
	copy(buf[begin:], data)
	return len(data), nil
}

func checkIntegrity(pw pieceWork, buf []byte) error {
	hash := sha1.Sum(buf)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("Index %d failed integrity check", pw.index)
	}
	return nil
}

func (p *PeerClient) SendHave(index int) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	msg := &Message{ID: MsgHave, Payload: payload}
	_, err := p.conn.Write(msg.Serialize())
	return err
}

// SendRequest sends a MsgRequest message to the peer
func (p *PeerClient) SendRequest(index, begin, length int) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	m := &Message{ID: MsgRequest, Payload: payload}
	_, err := p.conn.Write(m.Serialize())
	return err
}

func (p *PeerClient) sendUnchoke() error {
	msg := Message{ID: MsgUnChoke}
	_, err := p.conn.Write(msg.Serialize())
	return err
}

func (p *PeerClient) sendInterested() error {
	msg := Message{ID: MsgInterested}
	_, err := p.conn.Write(msg.Serialize())
	return err
}

func (p *PeerClient) handshake() error {
	log.Printf("start handshake with %s:%d\n", p.IP.String(), p.Port)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", p.IP.String(), p.Port), 3*time.Second)
	if err != nil {
		return err
	}

	p.conn = conn

	// 设置超时
	p.conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer p.conn.SetDeadline(time.Time{}) // Disable the deadline

	var buf bytes.Buffer
	buf.Write([]byte{19})
	buf.Write([]byte("BitTorrent protocol"))
	buf.Write(make([]byte, 8)) // reserved
	buf.Write(p.InfoHash[:])
	buf.Write(p.PeerID[:])
	conn.Write(buf.Bytes())

	lengthBuf := make([]byte, 1)
	_, err = io.ReadFull(conn, lengthBuf)
	if err != nil {
		return err
	}
	length := int(lengthBuf[0])
	if length == 0 {
		return errors.New("unexpected handshake length 0")
	}
	handshakeBuf := make([]byte, length+48)
	_, err = io.ReadFull(conn, handshakeBuf)
	if err != nil {
		return err
	}
	// compare info_hash
	if !bytes.Equal(handshakeBuf[length+8:length+8+20], p.InfoHash[:]) {
		return fmt.Errorf("info_hash not match, %s, %s", handshakeBuf[length+8:length+8+20], p.InfoHash[:])
	}
	log.Println("handshake success, ", fmt.Sprintf("%s:%d", p.IP.String(), p.Port))
	return nil
}

func (p *PeerClient) recvBitfield() {
	p.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer p.conn.SetDeadline(time.Time{}) // Disable the deadline
	m, err := ReadMessage(p.conn)
	if err != nil {
		return
	}
	if m.ID != MsgBitfield {
		panic("not bitfield")
	}
	p.bf = m.Payload
	p.Choked = true
}

// ReadMessage parses a message from a stream. Returns `nil` on keep-alive message
func ReadMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	// keep-alive message
	if length == 0 {
		return nil, nil
	}

	messageBuf := make([]byte, length)
	_, err = io.ReadFull(r, messageBuf)
	if err != nil {
		return nil, err
	}

	m := Message{
		ID:      MessageID(messageBuf[0]),
		Payload: messageBuf[1:],
	}

	return &m, nil
}

type pieceWork struct {
	index  int
	length int
	hash   [20]byte
}

type pieceResult struct {
	index int
	data  []byte

	peerIndex int
}

// Serialize serializes a message into a buffer of the form
// <length prefix><message ID><payload>
// Interprets `nil` as a keep-alive message
func (m *Message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4)
	}
	length := uint32(len(m.Payload) + 1) // +1 for id
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

func parsePeer(bin []byte) []Peer {
	peers := make([]Peer, len(bin)/6)
	if len(bin)%6 != 0 {
		panic("invalid peer data")
	}
	for i := 0; i < len(bin)/6; i++ {
		peers[i].IP = net.IP(bin[i*6 : i*6+4])
		peers[i].Port = binary.BigEndian.Uint16(bin[i*6+4 : i*6+6])
	}
	return peers
}

// Bitfields A Bitfield represents the pieces that a peer has
type Bitfields []byte

// HasPiece tells if a bitfield has a particular index set
func (bf Bitfields) HasPiece(index int) bool {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex < 0 || byteIndex >= len(bf) {
		return false
	}
	return bf[byteIndex]>>uint(7-offset)&1 != 0
}

// SetPiece sets a bit in the bitfield
func (bf Bitfields) SetPiece(index int) {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex < 0 || byteIndex >= len(bf) {
		return
	}
	bf[byteIndex] |= 1 << uint(7-offset)
}

func bencodeStruct(info info) []byte {
	buf := new(bytes.Buffer)
	bencode.Marshal(buf, info)
	return buf.Bytes()
}

func genRandomSelfPeerID() [20]byte {
	var SelfPeerID [20]byte
	_, _ = rand.Read(SelfPeerID[:])
	return SelfPeerID
}
