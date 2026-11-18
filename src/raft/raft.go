package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

var alivePeers int = 0

const timeoutEleicao time.Duration = 1000

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Estados persistentes
	CurrentTerm int
	VotedFor    *int
	logs        []ApplyMsg

	//Estados volateis em todos os servidores
	commitIndex  int
	lastApplied  int
	State        string
	ElectionTime time.Time
	dead         bool
	BirthTime    time.Time

	//Estados volateis em nós líderes (reinicializados após cada eleição)
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	if rf.State == "Leader" {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.VotedFor == nil || args.Term > rf.CurrentTerm || args.CandidateId == rf.me {
		if args.CandidateId != rf.me {
			rf.ElectionTime = time.Now()
		}
		reply.VoteGranted = true
		rf.VotedFor = &args.CandidateId
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
	} else {
		fmt.Printf("NÓ %v --- Respondendo %v --- Já votei em %v\n", rf.me, args.CandidateId, *rf.VotedFor)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if args.Term >= rf.CurrentTerm {
		rf.ElectionTime = time.Now()
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		//rf.State = "Follower"
	} else {
		reply.Term = rf.CurrentTerm
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) assertDominance() {
	var args = AppendEntryArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	for !rf.dead && rf.State == "Leader" {
		for server := 0; server < len(rf.peers); server += 1 {
			if server == rf.me {
				continue
			}
			var reply = AppendEntryReply{}
			rf.sendAppendEntry(server, &args, &reply)
			//fmt.Printf("Lider %v --- TermoLider %v --- TermoReply %v --- Servidor %v\n", rf.me, rf.CurrentTerm, reply.Term, server)
			if reply.Term > rf.CurrentTerm {
				fmt.Printf("============================EU(%v) VIREI UM SEGUIDOR========================\n", rf.me)
				rf.State = "Follower"
				rf.CurrentTerm = reply.Term
				rf.startElectionTimer()
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term int
}

func (rf *Raft) startElectionTimer() {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	rf.ElectionTime = time.Now()
	for !rf.dead {
		//SE DER O TEMPO DA ELEIÇÃO...
		time.Sleep(20 * time.Millisecond)
		if time.Now().Sub(rf.ElectionTime) >= timeoutEleicao*time.Millisecond {
			fmt.Printf("Tempo de espera do %v = %v\n", rf.me, time.Now().Sub(rf.BirthTime))
			if time.Now().Sub(rf.ElectionTime) >= timeoutEleicao*time.Millisecond {
				fmt.Printf("%v concorrerá a eleicao\n", rf.me)
				rf.runForElection()
			}
		}
	}
}

func (rf *Raft) runForElection() {
	// Atualizamos o termo pois ele está enviando o pedido de voto e mudamos o estado
	rf.mu.Lock()
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.State = "Candidate"

	// Adicionando valores ao argumento passado pelo RPC
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.CurrentTerm

	rf.VotedFor = &rf.me
	qtdVotos := 1
	votosValidos := 1

	var electionWaitGroup = sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		electionWaitGroup.Add(1)
		go func(rfIndex int, rf *Raft, args *RequestVoteArgs, qtdVotos *int, votosValidos *int, wg *sync.WaitGroup) {
			// Criando a estrutura que será utilizada para passagem dos parâmetros
			fmt.Printf("Eleicao de %v (termo líder = %v) --- Nó a votar = %v\n", rf.me, rf.CurrentTerm, rfIndex)
			reply := RequestVoteReply{}
			defer wg.Done()
			okReply := rf.sendRequestVote(rfIndex, args, &reply)
			if okReply == true && reply.VoteGranted == true {
				*votosValidos += 1
				*qtdVotos += 1
				fmt.Printf("Eleicao de %v (termo líder = %v) --- Nó votante = %v (termo nó %v)\n", rf.me, rf.CurrentTerm, rfIndex, reply.Term)
				if float64(*qtdVotos) > float64(len(rf.peers))/2 {
					rf.State = "Leader"
					fmt.Printf("Nó %v teve votos suficientes para se tornar líder\n", rf.me)
					rf.assertDominance()
					return
				}
			}
			if okReply == true && reply.VoteGranted == false {
				fmt.Printf("Eleicao de %v (termo líder = %v) --- Nó opositor %v (termo nó %v)\n", rf.me, rf.CurrentTerm, rfIndex, reply.Term)
				*votosValidos += 1
			} else if okReply == false {
				fmt.Printf("Servidor %v não retornou resposta válida\n", rfIndex)
			}
		}(i, rf, &args, &qtdVotos, &votosValidos, &electionWaitGroup)
	}
	electionWaitGroup.Wait()
	fmt.Printf("Quantidade de votos recebidos por %v = %v (total de peers ativos %v)\n", rf.me, qtdVotos, votosValidos)

	if rf.State != "Leader" {
		if float64(qtdVotos) > float64(len(rf.peers))/2 {
			rf.State = "Leader"
			rf.assertDominance()
		} else {
			rf.State = "Follower"
			go rf.startElectionTimer()
		}
	}
	rf.mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.dead = true
	alivePeers -= 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}            //Ponteiro para objeto Raft
	rf.peers = peers         //Slice de ponteiros para labrpc.ClientEnd (lista de quais peers existem)
	rf.persister = persister //Objeto que guarda o estado do peer
	rf.me = me               //O índice deste peer no slice de peers

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = nil
	rf.State = "Follower"
	alivePeers += 1
	rf.BirthTime = time.Now()
	go rf.startElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
