import argparse
import asyncio
import struct
import sys
import zlib
from typing import Callable, Optional, Tuple

PKT_DATA = 1
PKT_ACK = 2

# Header: Type (1 byte), Sequence Number (4 bytes), Checksum (4 bytes)
HEADER_STRUCT = struct.Struct("!BII")
Address = Tuple[str, int]


def compute_checksum(pkt_type: int, seq: int, payload: bytes) -> int:
    """Compute CRC32 over type + sequence + payload."""
    base = struct.pack("!BI", pkt_type, seq)
    return zlib.crc32(base + payload) & 0xFFFFFFFF


def build_packet(pkt_type: int, seq: int, payload: bytes = b"") -> bytes:
    checksum = compute_checksum(pkt_type, seq, payload)
    return HEADER_STRUCT.pack(pkt_type, seq, checksum) + payload


def parse_packet(data: bytes):
    if len(data) < HEADER_STRUCT.size:
        return None

    pkt_type, seq, recv_checksum = HEADER_STRUCT.unpack(data[: HEADER_STRUCT.size])
    payload = data[HEADER_STRUCT.size :]
    calc_checksum = compute_checksum(pkt_type, seq, payload)
    is_valid = recv_checksum == calc_checksum
    return pkt_type, seq, recv_checksum, payload, is_valid


class ReliableUDPProtocol(asyncio.DatagramProtocol):
    def __init__(
        self,
        on_message: Callable[[str, Address], None],
        timeout_ms: int = 500,
        debug: bool = True,
    ):
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.on_message = on_message
        self.timeout_ms = timeout_ms
        self.debug = debug

        # Stop-and-Wait state (sender)
        self.send_seq = 0
        self.pending_ack_seq: Optional[int] = None
        self.pending_ack_event: Optional[asyncio.Event] = None
        self.retransmit_task: Optional[asyncio.Task] = None
        self.send_lock = asyncio.Lock()
        self.retransmit_count = 0

        # Stop-and-Wait state (receiver)
        self.expected_seq = 0

        self.remote_addr: Optional[Address] = None

    def _log(self, msg: str) -> None:
        if self.debug:
            print(msg)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.retransmit_task is not None:
            self.retransmit_task.cancel()

    def datagram_received(self, data: bytes, addr: Address) -> None:
        parsed = parse_packet(data)
        if parsed is None:
            self._log(f"Pacote inválido (curto) de {addr}")
            return

        pkt_type, seq, _recv_checksum, payload, is_valid = parsed

        # Fiabilidade: validação de checksum para descartar pacotes corrompidos.
        if not is_valid:
            self._log(f"Checksum inválido para Seq {seq} (de {addr})")
            return

        if pkt_type == PKT_ACK:
            self._handle_ack(seq, addr)
            return

        if pkt_type == PKT_DATA:
            self._handle_data(seq, payload, addr)
            return

        self._log(f"Tipo de pacote desconhecido: {pkt_type}")

    def _handle_ack(self, seq: int, addr: Address) -> None:
        # Fiabilidade: aprende o endpoint real do peer a partir do ACK recebido.
        self.remote_addr = addr

        if self.pending_ack_seq is None:
            return

        if seq != self.pending_ack_seq:
            return

        self._log(f"ACK Recebido {seq}")
        if self.pending_ack_event is not None and not self.pending_ack_event.is_set():
            self.pending_ack_event.set()

    def _send_ack(self, seq: int, addr: Address) -> None:
        if self.transport is None:
            return
        ack_packet = build_packet(PKT_ACK, seq, b"")
        self._log(f"Enviando ACK {seq} para {addr[0]}:{addr[1]}")
        self.transport.sendto(ack_packet, addr)

    def _handle_data(self, seq: int, payload: bytes, addr: Address) -> None:
        # Fiabilidade: aprende o endpoint real do peer a partir de qualquer DATA válido.
        self.remote_addr = addr

        # Fiabilidade: ACK enviado para cada pacote de dados recebido (inclui duplicados).
        self._send_ack(seq, addr)

        if seq != self.expected_seq:
            # Fiabilidade: duplicado/out-of-order descartado para manter entrega em ordem.
            self._log(f"Duplicado/fora de ordem Seq {seq}, esperado {self.expected_seq}")
            return

        message = payload.decode("utf-8", errors="replace")
        self.on_message(message, addr)

        # Fiabilidade: só avança a sequência quando o pacote esperado chega corretamente.
        self.expected_seq ^= 1

    async def _retransmit_until_ack(
        self,
        seq: int,
        packet: bytes,
        addr: Address,
        ack_event: asyncio.Event,
    ) -> None:
        timeout_s = self.timeout_ms / 1000.0
        while not ack_event.is_set():
            await asyncio.sleep(timeout_s)
            if ack_event.is_set():
                break

            # Fiabilidade: timeout expirou sem ACK, retransmite o mesmo pacote.
            self.retransmit_count += 1
            self._log(f"Retransmitindo Seq {seq}")
            if self.transport is not None:
                self.transport.sendto(packet, addr)

    async def send_reliable(self, message: str, addr: Optional[Address] = None) -> None:
        if self.transport is None:
            raise RuntimeError("Transporte UDP ainda não está pronto.")

        target = addr or self.remote_addr
        if target is None:
            raise ValueError("Endereço remoto não definido.")

        payload = message.encode("utf-8")

        async with self.send_lock:
            seq = self.send_seq
            packet = build_packet(PKT_DATA, seq, payload)
            ack_event = asyncio.Event()

            self.pending_ack_seq = seq
            self.pending_ack_event = ack_event

            self._log(f"Enviando Seq {seq}")
            self.transport.sendto(packet, target)

            self.retransmit_task = asyncio.create_task(
                self._retransmit_until_ack(seq, packet, target, ack_event)
            )

            await ack_event.wait()

            if self.retransmit_task is not None:
                self.retransmit_task.cancel()
                try:
                    await self.retransmit_task
                except asyncio.CancelledError:
                    pass
                self.retransmit_task = None

            self.pending_ack_seq = None
            self.pending_ack_event = None

            # Fiabilidade: Stop-and-Wait usa alternância de sequência 0/1 por pacote confirmado.
            self.send_seq ^= 1


async def read_stdin_line() -> Optional[str]:
    loop = asyncio.get_running_loop()
    line = await loop.run_in_executor(None, sys.stdin.readline)
    if line == "":
        return None
    return line.rstrip("\n")


async def send_file_messages(rudp: ReliableUDPProtocol, file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue

            if text.lower() in {"/quit", "/exit"}:
                return

            await rudp.send_reliable(text)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Chat P2P sobre UDP Fiável (Stop-and-Wait)")
    parser.add_argument("--local-port", type=int, required=True)
    parser.add_argument("--remote-host", type=str, required=True)
    parser.add_argument("--remote-port", type=int, required=True)
    parser.add_argument("--timeout-ms", type=int, default=500)
    parser.add_argument(
        "--send-file",
        type=str,
        default=None,
        help="Ficheiro com uma mensagem por linha (usa /quit para terminar).",
    )
    args = parser.parse_args()

    def on_message(message: str, addr: Address) -> None:
        print(f"\n[{addr[0]}:{addr[1]}] {message}")

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ReliableUDPProtocol(on_message=on_message, timeout_ms=args.timeout_ms, debug=True),
        local_addr=("0.0.0.0", args.local_port),
    )

    rudp = protocol  # type: ReliableUDPProtocol
    rudp.remote_addr = (args.remote_host, args.remote_port)

    print(
        f"UDP fiável ativo em 0.0.0.0:{args.local_port} -> "
        f"{args.remote_host}:{args.remote_port}. Use /quit para sair."
    )

    try:
        if args.send_file:
            await send_file_messages(rudp, args.send_file)
        else:
            while True:
                line = await read_stdin_line()
                if line is None:
                    break

                text = line.strip()
                if not text:
                    continue

                if text.lower() in {"/quit", "/exit"}:
                    break

                await rudp.send_reliable(text)
    finally:
        print(f"Total de retransmissoes: {rudp.retransmit_count}")
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
