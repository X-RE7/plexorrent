import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import net, { type Socket } from 'node:net'
import type { BrowserWindow } from 'electron'
import { Notification } from 'electron'
import type { Torrent } from 'webtorrent'
import { IpcChannels } from '../../shared/ipc-channels'
import type {
  BlockState,
  ChunkState,
  DownloadState,
  NetworkInterfaceInfo,
  StartDownloadRequest
} from '../../shared/types'
import { connectFrom } from '../network/deviceBinding'

interface SpeedSample {
  bytes: number
  time: number
}

interface WireLike {
  peer?: { _interfaceId?: string }
  on: (event: string, callback: (bytes: number) => void) => void
}

interface PeerItem {
  addr: string
  type: string
  _interfaceId?: string
  conn?: Socket
  connectTimeout?: NodeJS.Timeout
  onConnect: () => void
  destroy: (err?: unknown) => void
}

interface WebTorrentClientLike {
  maxConns: number
  add: (url: string, opts?: unknown) => InternalTorrent
  destroy: (cb?: () => void) => void
}

type InternalTorrent = Torrent & {
  _drain: () => void
  _queue: PeerItem[]
  _numConns: number
  _numPending: number
  destroyed: boolean
  client: WebTorrentClientLike
}

async function createWebTorrentClient(
  options: Record<string, unknown>
): Promise<WebTorrentClientLike> {
  const dynamicImport = new Function('specifier', 'return import(specifier)')
  const mod = await dynamicImport('webtorrent')
  const ClientConstructor = mod.default || mod
  return new ClientConstructor(options) as WebTorrentClientLike
}

const SPEED_WINDOW_MS = 3000
const UPDATE_INTERVAL_MS = 250

function pushSpeedSample(samples: SpeedSample[], bytes: number, time: number): void {
  samples.push({ bytes, time })
  const cutoff = time - SPEED_WINDOW_MS
  while (samples.length > 2 && samples[1].time <= cutoff) {
    samples.shift()
  }
}

function calculateSpeed(samples: SpeedSample[], time: number): number {
  if (samples.length < 2) return 0
  const latest = samples[samples.length - 1]
  if (time - latest.time > SPEED_WINDOW_MS) return 0
  const oldest = samples[0]
  const deltaSec = Math.max((latest.time - oldest.time) / 1000, 1)
  return Math.max(0, (latest.bytes - oldest.bytes) / deltaSec)
}

function parseAddr(addr: string): [string, number] {
  const lastColon = addr.lastIndexOf(':')
  if (lastColon === -1) return [addr, 6881]
  const host = addr.slice(0, lastColon).replace(/^\[|\]$/g, '')
  const port = Number(addr.slice(lastColon + 1)) || 6881
  return [host, port]
}

export class TorrentManager {
  private client: WebTorrentClientLike | null = null
  private currentTorrent: InternalTorrent | null = null
  private state: DownloadState | null = null
  private activeInterfaces: NetworkInterfaceInfo[] = []
  private updateTimer: NodeJS.Timeout | null = null
  private interfaceBytes: Map<string, number> = new Map()
  private interfaceSamples: Map<string, SpeedSample[]> = new Map()
  private lastAttributedInterfaceId: string | null = null

  constructor(
    private getWindow: () => BrowserWindow | null,
    private getInterfaceById: (id: string) => NetworkInterfaceInfo | undefined
  ) {}

  isActive(): boolean {
    return (
      this.state !== null && (this.state.status === 'downloading' || this.state.status === 'paused')
    )
  }

  getCurrentDownload(): DownloadState | null {
    return this.state ? structuredClone(this.state) : null
  }

  async start(request: StartDownloadRequest): Promise<string> {
    if (this.isActive()) {
      throw new Error('A torrent download is already in progress.')
    }

    // Resolve selected interfaces
    this.activeInterfaces = request.interfaceIds
      .map((id) => this.getInterfaceById(id))
      .filter((iface): iface is NetworkInterfaceInfo => Boolean(iface))

    if (this.activeInterfaces.length === 0) {
      throw new Error('No valid network interface selected.')
    }

    this.interfaceBytes.clear()
    this.interfaceSamples.clear()
    for (const iface of this.activeInterfaces) {
      this.interfaceBytes.set(iface.id, 0)
      this.interfaceSamples.set(iface.id, [])
    }

    const downloadId = randomUUID()
    const chunks: ChunkState[] = this.activeInterfaces.map((iface, index) => ({
      id: index,
      interfaceId: iface.id,
      interfaceLabel: iface.displayName,
      interfaceKind: iface.kind,
      rangeStart: 0,
      rangeEnd: null,
      bytesDownloaded: 0,
      speedBytesPerSec: 0,
      status: 'downloading',
      retryCount: 0
    }))

    this.state = {
      id: downloadId,
      url: request.url,
      fileName: request.suggestedFileName || 'Torrent Download',
      destinationPath: join(request.destinationDir, request.suggestedFileName || 'download'),
      totalBytes: request.totalBytes || 0,
      bytesDownloaded: 0,
      speedBytesPerSec: 0,
      status: 'downloading',
      chunks,
      blocks: [],
      totalBlocks: 0,
      blockSizeBytes: 0,
      startedAt: Date.now(),
      isTorrent: true,
      peersCount: 0
    }

    this.pushState()

    // Initialize WebTorrent client
    this.client = await createWebTorrentClient({
      utp: false, // Force TCP for socket-level multi-interface binding
      dht: true,
      maxConns: 55
    })

    const torrent = this.client.add(request.url, {
      path: request.destinationDir
    }) as unknown as InternalTorrent
    this.currentTorrent = torrent

    // Multi-interface round-robin socket interceptor
    let roundRobin = 0
    const activeInterfaces = this.activeInterfaces

    torrent._drain = function () {
      if (
        typeof net.connect !== 'function' ||
        this.destroyed ||
        this.paused ||
        this._numConns + this._numPending >= this.client.maxConns
      ) {
        return
      }

      const peer = this._queue.shift()
      if (!peer) return

      const [host, port] = parseAddr(peer.addr)
      const iface = activeInterfaces[roundRobin++ % activeInterfaces.length]
      peer._interfaceId = iface.id

      let conn: Socket
      try {
        conn = connectFrom(iface.address, host, port)
      } catch {
        conn = net.connect({ host, port, localAddress: iface.address, family: 4 })
      }
      peer.conn = conn

      this._numPending += 1
      let done = false
      const donePending = (): void => {
        if (!done) {
          done = true
          this._numPending -= 1
        }
      }

      conn.once('connect', () => {
        donePending()
        if (!this.destroyed) peer.onConnect()
      })
      conn.once('error', (err: unknown) => {
        donePending()
        peer.destroy(err)
      })
      conn.once('close', donePending)

      peer.connectTimeout = setTimeout(() => {
        donePending()
        peer.destroy(new Error('connect timeout'))
      }, 5000)

      if (this._queue.length > 0) {
        process.nextTick(() => this._drain())
      }
    }

    // Peer wire attribution
    torrent.on('wire', (wire: WireLike) => {
      const ifaceId = wire.peer?._interfaceId || activeInterfaces[0].id
      wire.on('download', (bytes: number) => {
        this.lastAttributedInterfaceId = ifaceId
        const current = this.interfaceBytes.get(ifaceId) || 0
        const updated = current + bytes
        this.interfaceBytes.set(ifaceId, updated)
        const samples = this.interfaceSamples.get(ifaceId)
        if (samples) {
          pushSpeedSample(samples, updated, Date.now())
        }
      })
    })

    // Metadata ready
    torrent.on('metadata', () => {
      if (!this.state) return
      this.state.fileName = torrent.name
      this.state.destinationPath = join(request.destinationDir, torrent.name)
      this.state.totalBytes = torrent.length
      this.state.blockSizeBytes = torrent.pieceLength
      this.state.totalBlocks = torrent.pieces.length

      this.state.blocks = torrent.pieces.map((piece: unknown, index: number) => {
        const rangeStart = index * torrent.pieceLength
        const rangeEnd = Math.min(torrent.length, (index + 1) * torrent.pieceLength) - 1
        const isComplete = Boolean(piece)
        return {
          index,
          rangeStart,
          rangeEnd,
          status: isComplete ? 'completed' : 'pending',
          bytesDownloaded: isComplete ? rangeEnd - rangeStart + 1 : 0,
          bytesByInterface: {}
        } as BlockState
      })

      this.pushState()
    })

    // Piece verified and completed
    torrent.on('verified', (pieceIndex: number) => {
      if (!this.state || !this.state.blocks || !this.state.blocks[pieceIndex]) return
      const block = this.state.blocks[pieceIndex]
      block.status = 'completed'
      const pieceSize = block.rangeEnd !== null ? block.rangeEnd - block.rangeStart + 1 : 0
      block.bytesDownloaded = pieceSize
      const ifaceId = this.lastAttributedInterfaceId || activeInterfaces[0].id
      block.interfaceId = ifaceId
      block.bytesByInterface = { [ifaceId]: pieceSize }
    })

    // Completed
    torrent.on('done', () => {
      if (!this.state) return
      this.state.status = 'completed'
      this.state.completedAt = Date.now()
      this.state.bytesDownloaded = this.state.totalBytes
      this.state.speedBytesPerSec = 0
      for (const chunk of this.state.chunks) {
        chunk.status = 'completed'
        chunk.speedBytesPerSec = 0
      }
      this.pushState()
      try {
        new Notification({
          title: 'Download complete',
          body: this.state.fileName
        }).show()
      } catch {
        // Notification best-effort
      }
    })

    torrent.on('error', (err: unknown) => {
      if (!this.state) return
      this.state.status = 'error'
      this.state.error = err instanceof Error ? err.message : 'Torrent error occurred'
      this.pushState()
    })

    // Periodic telemetry loop
    this.startTelemetryLoop()

    return downloadId
  }

  private startTelemetryLoop(): void {
    if (this.updateTimer) clearInterval(this.updateTimer)
    this.updateTimer = setInterval(() => {
      if (!this.state || !this.currentTorrent) return
      if (this.state.status !== 'downloading') return

      const now = Date.now()
      let totalSpeed = 0

      // Update per-network chunks
      for (const chunk of this.state.chunks) {
        const bytes = this.interfaceBytes.get(chunk.interfaceId) || 0
        const samples = this.interfaceSamples.get(chunk.interfaceId) || []
        chunk.bytesDownloaded = bytes
        chunk.speedBytesPerSec = calculateSpeed(samples, now)
        totalSpeed += chunk.speedBytesPerSec
      }

      this.state.bytesDownloaded = this.currentTorrent.downloaded || 0
      this.state.speedBytesPerSec = totalSpeed || this.currentTorrent.downloadSpeed || 0
      this.state.peersCount = this.currentTorrent.numPeers || 0

      this.pushState()
    }, UPDATE_INTERVAL_MS)
  }

  async pause(): Promise<void> {
    if (!this.state || !this.currentTorrent) return
    this.currentTorrent.pause()
    this.state.status = 'paused'
    this.state.pausedAt = Date.now()
    this.state.speedBytesPerSec = 0
    for (const chunk of this.state.chunks) {
      chunk.status = 'paused'
      chunk.speedBytesPerSec = 0
    }
    this.pushState()
  }

  resume(): void {
    if (!this.state || !this.currentTorrent) return
    this.currentTorrent.resume()
    this.state.status = 'downloading'
    for (const chunk of this.state.chunks) {
      chunk.status = 'downloading'
    }
    this.pushState()
  }

  cancel(): void {
    if (this.updateTimer) {
      clearInterval(this.updateTimer)
      this.updateTimer = null
    }
    if (this.currentTorrent) {
      try {
        this.currentTorrent.destroy()
      } catch {
        // Best-effort
      }
      this.currentTorrent = null
    }
    if (this.client) {
      try {
        this.client.destroy()
      } catch {
        // Best-effort
      }
      this.client = null
    }
    if (this.state) {
      this.state.status = 'cancelled'
      this.pushState()
    }
  }

  remove(): void {
    this.cancel()
    this.state = null
  }

  private pushState(): void {
    if (!this.state) return
    const window = this.getWindow()
    if (window && !window.isDestroyed()) {
      window.webContents.send(IpcChannels.downloadUpdated, structuredClone(this.state))
    }
  }
}
