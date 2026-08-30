// Shared types mirroring the Go backend's JSON. Phase 0 only defines the
// connection handshake; game-state shapes land in later phases.

export interface HelloMessage {
  type: 'hello'
  msg: string
}

export type ServerMessage = HelloMessage

export interface ConnectionState {
  status: 'connecting' | 'connected' | 'disconnected'
  lastMessage?: ServerMessage
  error?: string
}
