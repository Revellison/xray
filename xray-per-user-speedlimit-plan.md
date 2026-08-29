# План патча: per-user rate limit в Xray-core

## Цель

Добавить в форк Xray-core ограничение скорости (up/down) на пользователя по `email`,
с конфигурацией через отдельную секцию `speedLimit` на корневом уровне JSON-конфига,
не пересекающуюся с полем `settings.clients` (его перезаписывает панель remnawave,
трогать его агенту нельзя).

Итоговый эффект: если у пользователя `speedLimit.userLimits["user123"] = {up: 10000, down: 10000}`
(килобит/сек), то весь его трафик — суммарно по всем активным соединениям — не может
превышать 10000 кбит/с на приём и 10000 кбит/с на отдачу. Данные не дропаются и
соединения не рвутся, а придерживаются по времени (token bucket).

---

## 0. Предварительные шаги (агент делает первым)

1. Определить версию Xray-core в форке: `git log -1 --oneline`, `git describe --tags`.
2. Найти актуальный путь до диспетчера — в разных версиях это может быть
   `app/dispatcher/default.go`, проверить командой:
   ```
   grep -rn "func (d \*DefaultDispatcher) Dispatch" app/dispatcher/
   ```
3. Найти, где в этой функции определяется `email` пользователя — обычно через
   `session.ContentFromContext(ctx)` или похожий вызов, искать:
   ```
   grep -rn "User.Email\|\.Email" app/dispatcher/default.go
   ```
   Если конкретной версии структура отличается — адаптировать пути ниже под
   реально найденные сигнатуры, не переименовывать существующие типы.
4. Найти структуру, где парсится JSON-конфиг верхнего уровня (`infra/conf/*.go`
   или `main/confloader`), чтобы понять, куда добавить парсинг нового корневого
   ключа `speedLimit`.

---

## 1. Новый пакет `app/limiter`

Создать директорию `app/limiter/` с файлами:

### `app/limiter/limiter.go`

Назначение: хранилище лимитеров по email + обёртка над `buf.Writer`, которая
тормозит запись по лимиту.

Содержимое (ориентир, адаптировать импорты под актуальный модуль):

```go
package limiter

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"github.com/xtls/xray-core/common/buf"
)

// Speed — лимит в байтах в секунду. Конвертация из kbps делается при парсинге конфига.
type Speed struct {
	Up   int64 // байт/сек, upload (клиент -> сервер -> outbound)
	Down int64 // байт/сек, download (сервер -> клиент)
}

type Limiter struct {
	mu       sync.RWMutex
	up       map[string]*rate.Limiter // email -> bucket для upload
	down     map[string]*rate.Limiter // email -> bucket для download
	config   map[string]Speed         // email -> лимит из конфига
	defaultL Speed                    // лимит по умолчанию, если email не в config (0 = не лимитировать)
}

func New() *Limiter {
	return &Limiter{
		up:     make(map[string]*rate.Limiter),
		down:   make(map[string]*rate.Limiter),
		config: make(map[string]Speed),
	}
}

// LoadConfig вызывается один раз при старте, засовывает распарсенный
// speedLimit.userLimits и speedLimit.default сюда.
func (l *Limiter) LoadConfig(userLimits map[string]Speed, def Speed) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.config = userLimits
	l.defaultL = def
}

func (l *Limiter) getBucket(store map[string]*rate.Limiter, email string, bps int64) *rate.Limiter {
	l.mu.RLock()
	lim, ok := store[email]
	l.mu.RUnlock()
	if ok {
		return lim
	}
	if bps <= 0 {
		return nil // без лимита
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// double-check на случай гонки между RUnlock и Lock
	if lim, ok := store[email]; ok {
		return lim
	}
	lim = rate.NewLimiter(rate.Limit(bps), int(bps)) // burst = 1 сек запаса
	store[email] = lim
	return lim
}

func (l *Limiter) speedFor(email string) Speed {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if s, ok := l.config[email]; ok {
		return s
	}
	return l.defaultL
}

// WrapUplinkWriter оборачивает writer, идущий в сторону outbound (то, что льёт пользователь).
func (l *Limiter) WrapUplinkWriter(email string, w buf.Writer) buf.Writer {
	s := l.speedFor(email)
	bucket := l.getBucket(l.up, email, s.Up)
	if bucket == nil {
		return w
	}
	return &RateWriter{writer: w, limiter: bucket}
}

// WrapDownlinkWriter оборачивает writer, идущий в сторону клиента (то, что клиент качает).
func (l *Limiter) WrapDownlinkWriter(email string, w buf.Writer) buf.Writer {
	s := l.speedFor(email)
	bucket := l.getBucket(l.down, email, s.Down)
	if bucket == nil {
		return w
	}
	return &RateWriter{writer: w, limiter: bucket}
}

type RateWriter struct {
	writer  buf.Writer
	limiter *rate.Limiter
}

func (w *RateWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	n := mb.Len()
	if n > 0 {
		// burst бакета = его rate, поэтому WaitN с n больше burst будет
		// всегда возвращать ошибку "exceeds limiter burst" — надо резать
		// n на чанки не больше burst, если протокол может слать большие MultiBuffer.
		if err := waitN(w.limiter, int(n)); err != nil {
			return err
		}
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *RateWriter) Close() error {
	if c, ok := w.writer.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// waitN дробит запрос токенов на куски не больше burst лимитера, чтобы не
// словить ErrBurstExceeded на больших MultiBuffer.
func waitN(lim *rate.Limiter, n int) error {
	burst := lim.Burst()
	ctx := context.Background()
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := lim.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
```

**Зачем нужен `waitN` с дроблением**: `rate.Limiter.WaitN` возвращает ошибку, если
`n` больше `Burst()`. Один `MultiBuffer` теоретически может быть больше burst'а
(особенно на низких лимитах — например лимит 100 kbps даёт burst ~12500 байт, а
буфер может быть больше). Без дробления патч будет падать с ошибкой записи на
любом всплеске трафика — это частая причина "патч вроде работает, но рвёт
соединения на скачивании больших файлов", если её не учесть с самого начала.

---

## 2. Хук в диспетчере

Файл: `app/dispatcher/default.go` (или найденный на шаге 0.2 актуальный путь).

### 2.1. Добавить глобальный/инжектируемый инстанс лимитера

Найти конструктор `DefaultDispatcher` (обычно `NewDefaultDispatcher` или метод
`Init`, регистрируемый через DI-контейнер `common.NewComponent`/`core.Config`).
Добавить туда поле:

```go
type DefaultDispatcher struct {
	// ...существующие поля...
	speedLimiter *limiter.Limiter
}
```

Инициализировать в конструкторе:
```go
d.speedLimiter = limiter.New()
```

Загрузку конфига (`LoadConfig`) вызвать там, где диспетчер получает доступ к
общему `core.Config` — как правило это делается в `Init(config *DispatcherConfig, ...)`
или в top-level `core.New(...)`. Найти по:
```
grep -rn "func.*DefaultDispatcher.*Init\|NewDefaultDispatcher" app/dispatcher/
```

### 2.2. Точка оборачивания Writer'ов

Найти в `Dispatch(ctx context.Context, destination net.Destination, ...)` (имя и
сигнатура могут отличаться версии от версии — искать по созданию `transport.Link{}`)
место, где создаются `outboundLink` и `inboundLink`, например:

```go
// было (ориентировочно, реальный код может отличаться порядком полей):
inboundLink := &transport.Link{
	Reader: inboundReader,
	Writer: inboundWriter,
}
outboundLink := &transport.Link{
	Reader: outboundReader,
	Writer: outboundWriter,
}
```

Добавить сразу после этого блока:

```go
if content := session.ContentFromContext(ctx); content != nil && content.User != nil {
	email := content.User.Email
	if email != "" {
		inboundLink.Writer = d.speedLimiter.WrapDownlinkWriter(email, inboundLink.Writer)
		outboundLink.Writer = d.speedLimiter.WrapUplinkWriter(email, outboundLink.Writer)
	}
}
```

**Важно про направления**: убедиться на месте, что `inboundLink.Writer` — это
действительно поток "к клиенту" (download с точки зрения пользователя), а
`outboundLink.Writer` — поток "в интернет" (upload). В разных версиях диспетчера
именование `inbound`/`outbound` может относиться либо к линку, либо к его
источнику — нужно явно протрассировать по коду (или добавить временный лог
`log.Println` с направлением и destination при первом тесте), а не полагаться
на предположение по названию переменной, иначе `up`/`down` в конфиге поменяются
местами и лимиты будут перепутаны.

---

## 3. Парсинг конфига: корневой ключ `speedLimit`

### 3.1. Формат JSON (справочно, уже согласован с пользователем)

```json
{
  "speedLimit": {
    "userLimits": {
      "user123": { "up": 10000, "down": 10000 },
      "user456": { "up": 5000,  "down": 20000 }
    },
    "default": { "up": 10000, "down": 10000 }
  }
}
```

Единицы — **килобиты в секунду**, целые числа. `default` — опционален, если
отсутствует, пользователи без явного лимита не ограничиваются (значение 0 —
специальный маркер "не лимитировать", учитывать при конвертации в байты).

### 3.2. Куда добавить парсинг

Найти файл, где парсится top-level конфиг (`infra/conf/config.go` — обычно
структура `Config` с полями типа `LogConfig`, `RouterConfig`, `DNSConfig` и т.п.,
которые потом собираются в `core.Config` через `Build()`).

Добавить:

1. Новую структуру в `infra/conf/`:

```go
type SpeedLimitEntry struct {
	Up   int64 `json:"up"`
	Down int64 `json:"down"`
}

type SpeedLimitConfig struct {
	UserLimits map[string]SpeedLimitEntry `json:"userLimits"`
	Default    *SpeedLimitEntry           `json:"default"`
}
```

2. Поле в главной структуре конфига:
```go
type Config struct {
	// ...существующие поля...
	SpeedLimitConfig *SpeedLimitConfig `json:"speedLimit"`
}
```

3. В методе `Build()` (собирает `infra/conf.Config` в рантайм-конфиг ядра)
   — конвертировать kbps в байты/сек (`kbps * 1000 / 8`) и передать дальше
   тем механизмом, которым диспетчер получает доступ к общим настройкам
   (через `core.Config.App` как отдельный `AppConfig`, аналогично тому, как
   зарегистрированы другие app-модули — Router, DNS и т.д.). Если в конкретной
   версии ядра нет удобного generic-механизма для "произвольных данных на
   диспетчер" — самый простой путь: сделать `limiter.Limiter` синглтоном
   пакета (`var Global = New()`) и звать `limiter.Global.LoadConfig(...)`
   прямо из `Build()`, а в диспетчере ссылаться на `limiter.Global` вместо
   поля структуры. Это менее "чисто" архитектурно, но кардинально проще и
   меньше риск конфликтов при апдейте ядра, так как не трогает DI-механизм
   ядра вообще.

---

## 4. go.mod

Проверить, что `golang.org/x/time` уже есть в зависимостях:
```
grep "golang.org/x/time" go.mod
```
Если нет — добавить:
```
go get golang.org/x/time@latest
```

---

## 5. Сборка и юнит-тест

1. `go build ./...` — убедиться, что всё компилируется.
2. Написать минимальный тест в `app/limiter/limiter_test.go`:
   - создать `Limiter`, `LoadConfig` с одним юзером на 800 kbps (=100000 байт/сек),
   - обернуть тестовый `buf.Writer`-заглушку, писать в цикле по 200000 байт,
   - замерить время выполнения — должно быть заметно больше 1 секунды
     (грубая проверка, что throttling действительно работает, а не пропускает
     всё мгновенно).
3. Ручной прогон на тестовом сервере:
   - поднять с конфигом, где для тестового email стоит `up/down: 1000` (1 Мбит),
   - скачать/залить файл через `curl`/`speedtest` с клиента,
   - убедиться, что скорость стабильно упирается в ~1 Мбит, без обрывов
     соединения (это отличает throttling от "banning"-подхода, который
     обсуждался и был отвергнут ранее).

---

## 6. Что НЕ делать (ограничения задачи)

- Не трогать `settings.clients` ни в одном inbound — это зона ответственности
  панели remnawave, любые изменения там будут перезатёрты при следующей
  синхронизации.
- Не пытаться завязываться на per-connection лимиты — лимитер должен жить на
  уровне email, шариться между всеми соединениями одного пользователя.
- Не менять сигнатуры существующих публичных функций диспетчера без
  необходимости — если можно обойтись добавлением новых полей/веток кода без
  правки существующих сигнатур, выбирать этот путь: так патч проще накатывать
  повторно после апдейта ядра (см. раздел про git rebase ниже).

---

## 7. Организация патча для будущих обновлений ядра

- Держать это не как ручной diff, а как git-ветку `feature/speedlimit` поверх
  `upstream/main` в форке.
- При обновлении: `git fetch upstream && git rebase upstream/main`.
- Конфликты возможны только в двух точках: (а) блок с оборачиванием
  `inboundLink.Writer`/`outboundLink.Writer` в `Dispatch()`, (б) структура
  `Config` в `infra/conf/config.go`, если апстрим переименует соседние поля.
  Сам пакет `app/limiter/` — новый файл, конфликтовать не с чем.
