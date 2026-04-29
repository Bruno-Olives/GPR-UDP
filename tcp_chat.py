import argparse
import asyncio
import sys
from typing import Optional


async def read_stdin_line() -> Optional[str]:
    loop = asyncio.get_running_loop()
    line = await loop.run_in_executor(None, sys.stdin.readline)
    if line == "":
        return None
    return line.rstrip("\n")


async def send_file_messages(chat: "TCPPeerChat", file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue

            if text.lower() in {"/quit", "/exit"}:
                return

            await chat.send_message(text)


class TCPPeerChat:
    def __init__(self, remote_host: str, remote_port: int):
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.out_writer: Optional[asyncio.StreamWriter] = None
        self.connected_event = asyncio.Event()

    async def connect_outgoing(self) -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.remote_host, self.remote_port)
                self.out_writer = writer
                self.connected_event.set()
                print(f"Ligação TCP estabelecida para {self.remote_host}:{self.remote_port}")

                # Mantém o reader vivo para detetar fecho remoto.
                while True:
                    data = await reader.readline()
                    if not data:
                        break
                print("Ligação de saída fechada pelo remoto.")

            except OSError:
                print("Ainda sem ligação TCP ao remoto, a tentar novamente...")
                await asyncio.sleep(1.0)
            finally:
                self.connected_event.clear()
                if self.out_writer is not None:
                    self.out_writer.close()
                    await self.out_writer.wait_closed()
                    self.out_writer = None

    async def handle_incoming(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        print(f"Cliente TCP ligado: {peer}")

        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                message = data.decode("utf-8", errors="replace").rstrip("\n")
                print(f"\n[{peer}] {message}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def send_message(self, message: str) -> None:
        await self.connected_event.wait()
        if self.out_writer is None:
            return

        self.out_writer.write((message + "\n").encode("utf-8"))
        await self.out_writer.drain()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Chat P2P simples com TCP nativo")
    parser.add_argument("--local-port", type=int, required=True)
    parser.add_argument("--remote-host", type=str, required=True)
    parser.add_argument("--remote-port", type=int, required=True)
    parser.add_argument(
        "--send-file",
        type=str,
        default=None,
        help="Ficheiro com uma mensagem por linha (usa /quit para terminar).",
    )
    args = parser.parse_args()

    chat = TCPPeerChat(args.remote_host, args.remote_port)

    server = await asyncio.start_server(chat.handle_incoming, "0.0.0.0", args.local_port)
    print(
        f"Servidor TCP ativo em 0.0.0.0:{args.local_port}. "
        f"Destino: {args.remote_host}:{args.remote_port}. Use /quit para sair."
    )

    connect_task = asyncio.create_task(chat.connect_outgoing())

    try:
        if args.send_file:
            await send_file_messages(chat, args.send_file)
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

                await chat.send_message(text)
    finally:
        connect_task.cancel()
        try:
            await connect_task
        except asyncio.CancelledError:
            pass

        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
