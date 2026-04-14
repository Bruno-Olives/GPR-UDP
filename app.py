import asyncio

class ReliableUDPProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None
        self.pending_acks = {} # {seq: asyncio.TimerHandle}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # Lógica: Se for ACK, cancela o timer. 
        # Se for Dados, envia ACK e passa para a aplicação.
        pass

    def send_reliable(self, data, addr):
        seq = self.get_next_seq()
        self.transport.sendto(self.wrap_packet(seq, data), addr)
        # Setup do Timeout
        loop = asyncio.get_running_loop()
        timer = loop.call_later(0.5, self.retransmit, seq, data, addr)
        self.pending_acks[seq] = timer