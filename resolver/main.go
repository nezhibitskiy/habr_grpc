package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/NebulousLabs/fastrand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	pb "habr_article/helloworld/helloworld"
)

const (
	lookupServerHostPort = "localhost:8081"
)

type ResolverBuilder struct{}

func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Resolver{
		target: target.Endpoint(),
		ctx:    ctx,
		cancel: cancel,
		wg:     sync.WaitGroup{},
		cc:     cc,
	}

	r.wg.Add(1)
	go r.watch()

	return r, nil
}

func (b *ResolverBuilder) Scheme() string {
	return "habr"
}

type Resolver struct {
	target string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	cc     resolver.ClientConn
}

func (r *Resolver) watch() {
	defer r.wg.Done()
	r.lookup(r.target)
	ticker := time.NewTicker(50 * time.Second)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.lookup(r.target)
		}
	}
}

var defaultServiceConfig *serviceconfig.ParseResult

type (
	Response struct {
		Endpoints      []Endpoint     `json:"endpoints"`
		VersionWeights map[string]int `json:"version_weights"`
		ServiceConfig  string         `json:"service_config"`
	}
	Endpoint struct {
		Address string `json:"address"`
		Version string `json:"version"`
		// для WRR
		Weight int `json:"weight"`
	}
)

const (
	versionAttrKey  = "version"
	weightAttrKey   = "weight"
	weightsAttrsKey = "weight_by_version"
)

func (r *Resolver) lookup(target string) {
	resp, err := http.Get(fmt.Sprintf("http://%s/endpoints?target=%s", lookupServerHostPort, target))
	if err != nil {
		log.Println(err)
		return
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return
	}
	var epsResp Response
	err = json.Unmarshal(data, &epsResp)
	if err != nil {
		return
	}

	log.Println("resolver made request:", epsResp)
	newAddrs := make([]resolver.Address, 0, len(epsResp.Endpoints))
	for _, ep := range epsResp.Endpoints {
		newAddrs = append(newAddrs, resolver.Address{
			Addr:       ep.Address,
			Attributes: attributes.New(versionAttrKey, ep.Version).WithValue(weightAttrKey, ep.Weight),
		})
	}
	err = r.cc.UpdateState(resolver.State{
		Addresses:     newAddrs,
		ServiceConfig: r.cc.ParseServiceConfig(epsResp.ServiceConfig),
		Attributes:    attributes.New(weightsAttrsKey, epsResp.VersionWeights),
	})
	if err != nil {
		return
	}
}

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.lookup(r.target)
}

func (r *Resolver) Close() {
	r.cancel()
	r.wg.Wait()
}

type BalancerBuilder struct{}

func (bb BalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	bal := &Balancer{
		cc: cc,

		subConns: resolver.NewAddressMap(),
		scStates: make(map[balancer.SubConn]connectivity.State),
		csEvltr:  &balancer.ConnectivityStateEvaluator{},
		state:    connectivity.Connecting,
	}
	return bal
}

func (b BalancerBuilder) Name() string {
	return "habr_balancer"
}

// Важная инфа
// Функции UpdateClientConnState, ResolverError, UpdateSubConnState, и Close
// гарантированно вызываются синхронно в одной горутине, но нет гарантий
// на picker.Pick, так как он может быть вызван в любое время

type Balancer struct {
	cc      balancer.ClientConn
	csEvltr *balancer.ConnectivityStateEvaluator
	state   connectivity.State

	subConns          *resolver.AddressMap
	scStates          map[balancer.SubConn]connectivity.State
	picker            balancer.Picker
	lastResolverState resolver.State

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

func (b *Balancer) createNewSubConns(s balancer.ClientConnState) *resolver.AddressMap {
	// addrsSet это мапка, благодаря которой проще будет удалять в последующем сабконнекты, которые резолвер в новом обновлении не вернул
	addrsSet := resolver.NewAddressMap()
	for _, a := range s.ResolverState.Addresses {
		addrsSet.Set(a, nil)
		if _, ok := b.subConns.Get(a); !ok {
			// Создаем сабконнект, так как его в стейте балансера нет
			sc, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{})
			if err != nil {
				// Если что-то пошло не так - скипаем адрес и продолжаем
				// Возможные ошибки:
				// -- len(addrs) <= 0
				// -- ErrClientConnClosing
				// -- channelz.RegisterSubChannel error (errors.New("a SubChannel's parent id cannot be nil"))
				continue
			}
			// Сохраняем сабконнект
			b.subConns.Set(a, sc)

			b.scStates[sc] = connectivity.Idle
			// Так как это новый сабконнект - считаем, что ранее он был в выключенном состоянии
			b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)
			// Подключаемся (под капотом подключение в отдельной горутине происходит, поэтому просто вызываем функцию)
			sc.Connect()
		}
	}
	return addrsSet
}

func (b *Balancer) clearDeletedSubConns(addrsSet *resolver.AddressMap) {
	// Пройдемся по всем сабконнектам в текущем состоянии и проверим, не были ли сабконнекты удалены в новом состоянии резолвера
	for _, a := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(a)
		sc := sci.(balancer.SubConn)
		// проверяем наличие адреса в мапке новых адресов
		if _, ok := addrsSet.Get(a); !ok {
			// Если в новом стейте нет - делаем запрос у clientConn на удаление
			b.cc.RemoveSubConn(sc)
			b.subConns.Delete(a)
			// Но изменение собственного состояния произойдет, когда clientConn вернет колбэк по изменению состояния
			// этого сабконнекта через функцию UpdateSubConnState
		}
	}
}

func (b *Balancer) UpdateClientConnState(s balancer.ClientConnState) error {
	// В случае обновления очищаем ошибку резолвера (в конце будет новая проверка на неё)
	b.resolverErr = nil

	b.lastResolverState = s.ResolverState

	addrsSet := b.createNewSubConns(s)

	b.clearDeletedSubConns(addrsSet)

	// Если состояние резолвера не содержит адресов, то возвращаем ошибку, чтобы ClientConn попытался вызвать перерезолв
	// Также сохраняем ошибку об отсутствии адресов, чтобы в случае падения состояния балансера репортить её наверх

	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// В случае, если все ок - пересоздаем пикер и обновляем состояние балансера + высылаем новый пикер
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// ResolverError вызывается в случае, если в состоянии от резолвера содержалась ошибка (например, выслал 0 адресов для подключения)
// Данная функция может быть вызвана вышестоящими балансерами
func (b *Balancer) ResolverError(err error) {
	// сохраняем ошибку в стейт
	b.resolverErr = err

	// если у балансера нет сабконнектов, переводим его в состояние TransientFailure
	if b.subConns.Len() == 0 {
		b.state = connectivity.TransientFailure
	}

	// Если состояние не в TransientFailure => пикер должен дальше работать, пока балансер не будет отдавать корректную ошибку
	if b.state != connectivity.TransientFailure {
		return
	}

	// Пересоздаем пикер и обновляем состояние clientConn
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

func (b *Balancer) regeneratePicker() {
	// Если мы в состоянии TransientFailure => пикеру на каждый пик следует отдавать ошибку (мерж двух ошибок - резолвера и сабконнектов)
	if b.state == connectivity.TransientFailure {
		b.picker = NewErrPicker(b.mergeErrors())
		return
	}

	// Создаем мапу сабконнектов в Ready состоянии
	readySCs := make(map[balancer.SubConn]base.SubConnInfo)

	// Фильтруем все Ready сабконнекты из мапы всех сабконнектов
	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		if st, ok := b.scStates[sc]; ok && st == connectivity.Ready {
			readySCs[sc] = base.SubConnInfo{Address: addr}
		}
	}

	// Если в атрибутах имеются сведения о весах по версиям - создадим канареечный пикер
	if weightByVersion, ok := b.lastResolverState.Attributes.Value(weightsAttrsKey).(map[string]int); ok {
		b.picker = NewCanaryPicker(base.PickerBuildInfo{ReadySCs: readySCs}, weightByVersion)
	} else {
		// Создаем дефолтный RoundRobin пикер, который в функциях выше будет отправлен в состояние clientConn
		b.picker = BuildRRPicker(base.PickerBuildInfo{ReadySCs: readySCs})
	}
}

func (b *Balancer) mergeErrors() error {
	// Либо у нас проблема подключения, если есть какие-то сабконнекты,
	// либо у нас проблема резолвера, если нет сабконнектов
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	// Потенциально могут быть обе ошибки, если в логике резолвера есть более сложная логика, которая упала помимо
	// того, что он не смог создать сабконнекты (например, был совершен фолбэк на дополнительный механизм резолвинга)
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

func (b *Balancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	// Если к нам пришло обновление сабконнекта, которого нет в нашем стейте - скипаем
	oldS, ok := b.scStates[sc]
	if !ok {
		return
	}

	s := state.ConnectivityState

	if oldS == connectivity.TransientFailure &&
		(s == connectivity.Connecting || s == connectivity.Idle) {
		// Если subconn попал в TRANSIENT_FAILURE, игнорируем последующий IDLE или
		// CONNECTING переходы, чтобы предотвратить агрегированное состояние от
		// нахождения в постоянном CONNECTING состояние, когда много бэкендов существует,
		// но все они в нерабочем состоянии
		if s == connectivity.Idle {
			sc.Connect()
		}
		return
	}
	b.scStates[sc] = s
	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		// В этом месте удаляем сабконнект, который ранее мы попросили закрыть у clientConn через RemoveSubConn
		delete(b.scStates, sc)
	case connectivity.TransientFailure:
		// Если сабконнект попал в TransientFailure => сохраняем ошибку, по которой он находится в этом состоянии
		b.connErr = state.ConnectionError
	}

	// Записываем новое состояние в агрегатор состояний и получаем новое состояние всего балансера
	b.state = b.csEvltr.RecordTransition(oldS, s)

	// Пересоздаем пикер в следующих случаях:
	//  - сабконнект стал Ready или наоборот
	//  - Суммарное состояние балансера стало TransientFailure (возможно нужно обновить ошибку балансера)
	//    (may need to update error message)
	if (s == connectivity.Ready) != (oldS == connectivity.Ready) ||
		b.state == connectivity.TransientFailure {
		b.regeneratePicker()
	}
	// Обновляем стейт
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// ExitIdle используется в случае, если clientConn необходимо форсировать
// переход всех сабконнектов в состояние подключения. В нашем балансере мы не
// реализуем логику с каким-либо ожиданием перед тем, как подключиться к сабконнектам
// после того, как получаем обновление от Resolver
func (b *Balancer) ExitIdle() {}

// Close нужен для очистки внутреннего состояния балансера, если оно имеется
// Сабконнеты в Close закрывать не нужно
func (b *Balancer) Close() {}

func main() {
	rb := &ResolverBuilder{}
	resolver.Register(rb)

	bb := &BalancerBuilder{}
	balancer.Register(bb)

	conn, err := grpc.Dial("habr:///helloworld_server",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	time.Sleep(time.Second)

	c := pb.NewGreeterClient(conn)
	for i := 0; i < 10; i++ {
		// Contact the server and print out its response.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		go func() {
			r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "OzonTech"})
			if err != nil {
				log.Fatalf("could not greet: %v", err)
			}
			log.Printf("Greeting: %s", r.GetMessage())
		}()
		time.Sleep(2 * time.Second)
	}
}

func NewErrPicker(err error) balancer.Picker {
	return &errPicker{err: err}
}

type errPicker struct {
	err error
}

func (p *errPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{}, p.err
}

func BuildRRPicker(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	scs := make([]balancer.SubConn, 0, len(info.ReadySCs))
	for sc := range info.ReadySCs {
		scs = append(scs, sc)
	}
	return &rrPicker{
		subConns: scs,
		// Start at a random index, as the same RR balancer rebuilds a new
		// picker when SubConn states change, and we don't want to apply excess
		// load to the first server in the list.
		next: uint32(rand.Intn(len(scs))),
	}
}

type rrPicker struct {
	subConns []balancer.SubConn
	next     uint32
}

func (p *rrPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	subConnsLen := uint32(len(p.subConns))
	nextIndex := atomic.AddUint32(&p.next, 1)

	sc := p.subConns[nextIndex%subConnsLen]
	return balancer.PickResult{SubConn: sc}, nil
}

type (
	wrappedPicker struct {
		weight int
		picker balancer.Picker
	}

	CanaryPicker struct {
		totalWeight      int
		byVersionPickers []wrappedPicker
	}
)

func NewCanaryPicker(info base.PickerBuildInfo, weightsByVersion map[string]int) balancer.Picker {

	byVersion := make(map[string]map[balancer.SubConn]base.SubConnInfo)

	for subConn, connInfo := range info.ReadySCs {
		v, ok := connInfo.Address.Attributes.Value(versionAttrKey).(string)
		if !ok {
			continue
		}

		m := byVersion[v]
		if m == nil {
			m = make(map[balancer.SubConn]base.SubConnInfo)
		}
		m[subConn] = connInfo
		byVersion[v] = m
	}

	switch len(byVersion) {
	case 0:
		return NewErrPicker(balancer.ErrNoSubConnAvailable)
	case 1:
		// не канарейка :)
		for version, _ := range weightsByVersion {
			return BuildRRPicker(base.PickerBuildInfo{ReadySCs: byVersion[version]})
		}
	}

	byVersionPickers := make([]wrappedPicker, 0, len(byVersion))
	var totalWeight int
	for version, w := range weightsByVersion {
		totalWeight += w
		byVersionPickers = append(byVersionPickers, wrappedPicker{
			weight: w,
			picker: NewLeastConnPicker(base.PickerBuildInfo{ReadySCs: byVersion[version]}),
		})
	}

	return &CanaryPicker{
		totalWeight:      totalWeight,
		byVersionPickers: byVersionPickers,
	}
}

func (p *CanaryPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	weight := fastrand.Intn(p.totalWeight)

	for _, version := range p.byVersionPickers {
		weight -= version.weight
		if weight < 0 {
			return version.picker.Pick(info)
		}
	}

	if len(p.byVersionPickers) == 0 {
		return balancer.PickResult{}, errors.New("picker has no versions available")
	}

	return p.byVersionPickers[0].picker.Pick(info)
}

type (
	WRRPicker struct {
		wrr *RandomWRR
	}
)

func NewWRRPicker(info base.PickerBuildInfo) balancer.Picker {
	p := &WRRPicker{
		wrr: NewRandom(),
	}

	for subConn, connInfo := range info.ReadySCs {
		w, ok := connInfo.Address.Attributes.Value(weightAttrKey).(int)
		if !ok {
			// Если аттрибута нет, то выставляем 0 вес
			w = 0
		}
		// В случае, если у всех subConn вес нулевой - WRR выродится в Random
		p.wrr.Add(subConn, w)
	}
	return p
}

func (p *WRRPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	subConn, ok := p.wrr.Next().(balancer.SubConn)
	if !ok {
		return balancer.PickResult{}, errors.New("WRRPicker internal state error. Invalid item")
	}
	return balancer.PickResult{SubConn: subConn}, nil
}

// weightedItem is a wrapped weighted item that is used to implement weighted random algorithm.
type weightedItem struct {
	item              any
	weight            int
	accumulatedWeight int
}

func (w *weightedItem) String() string {
	return fmt.Sprint(*w)
}

// RandomWRR is a struct that contains weighted items implement weighted random algorithm.
type RandomWRR struct {
	items []*weightedItem
	// Are all item's weights equal
	equalWeights bool
}

// NewRandom creates a new WRR with random.
func NewRandom() *RandomWRR {
	return &RandomWRR{}
}

func (rw *RandomWRR) Next() (item any) {
	if len(rw.items) == 0 {
		return nil
	}
	if rw.equalWeights {
		return rw.items[fastrand.Intn(len(rw.items))].item
	}

	sumOfWeights := rw.items[len(rw.items)-1].accumulatedWeight
	// Random number in [0, sumOfWeights).
	randomWeight := fastrand.Intn(sumOfWeights)
	// Item's accumulated weights are in ascending order, because item's weight >= 0.
	// Binary search rw.items to find first item whose accumulatedWeight > randomWeight
	// The return i is guaranteed to be in range [0, len(rw.items)) because randomWeight < last item's accumulatedWeight
	i := sort.Search(len(rw.items), func(i int) bool { return rw.items[i].accumulatedWeight > randomWeight })
	return rw.items[i].item
}

func (rw *RandomWRR) Add(item any, weight int) {
	accumulatedWeight := weight
	equalWeights := true
	if len(rw.items) > 0 {
		lastItem := rw.items[len(rw.items)-1]
		accumulatedWeight = lastItem.accumulatedWeight + weight
		equalWeights = rw.equalWeights && weight == lastItem.weight
	}
	rw.equalWeights = equalWeights
	rItem := &weightedItem{item: item, weight: weight, accumulatedWeight: accumulatedWeight}
	rw.items = append(rw.items, rItem)
}

// leastConnItem is a wrapped weighted item that is used to implement least conn algorithm.
type leastConnItem struct {
	item        balancer.SubConn
	inFlightReq atomic.Int64
}

type LeastConnPicker struct {
	lc *leastConnections
}

func NewLeastConnPicker(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return NewErrPicker(errors.New("no subConn available"))
	}
	var items []balancer.SubConn
	for subConn := range info.ReadySCs {
		items = append(items, subConn)
	}

	p := &LeastConnPicker{
		lc: New(items),
	}

	return p
}

func (p *LeastConnPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	subConn, doneFn := p.lc.Next()
	if subConn == nil {
		return balancer.PickResult{}, errors.New("LeastConnPicker internal state error. No subConns in leastConn state")
	}
	return balancer.PickResult{SubConn: subConn, Done: doneFn}, nil
}

type leastConnections struct {
	conns []*leastConnItem
}

func New(items []balancer.SubConn) *leastConnections {
	conns := make([]*leastConnItem, 0, len(items))
	for _, i := range items {
		conns = append(conns, &leastConnItem{
			item:        i,
			inFlightReq: atomic.Int64{},
		})
	}
	return &leastConnections{
		conns: conns,
	}
}

func (lc *leastConnections) Next() (balancer.SubConn, func(info balancer.DoneInfo)) {
	if len(lc.conns) == 0 {
		return nil, nil
	}

	var (
		minInFlightReq = int64(-1)
		idx            int
	)

	for i, conn := range lc.conns {
		inFlightReq := conn.inFlightReq.Load()
		if minInFlightReq == -1 || inFlightReq < minInFlightReq {
			minInFlightReq = inFlightReq
			idx = i
		}
	}

	lc.conns[idx].inFlightReq.Add(1)

	return lc.conns[idx].item, func(balancer.DoneInfo) {
		lc.conns[idx].inFlightReq.Add(-1)
	}
}
