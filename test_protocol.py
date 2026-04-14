import asyncio

import pytest

from reliable_udp_chat import (
    PKT_ACK,
    PKT_DATA,
    ReliableUDPProtocol,
    parse_packet,
)


class InMemoryTransport(asyncio.DatagramTransport):
    def __init__(self, name, peer_protocol, local_addr, dropper=None):
        self.name = name
        self.peer_protocol = peer_protocol
        self.local_addr = local_addr
        self.dropper = dropper
        self.sent_packets = []
        self.closed = False

    def sendto(self, data, addr=None):
        self.sent_packets.append(data)
        if self.dropper and self.dropper(self.name, data):
            return
        self.peer_protocol.datagram_received(data, self.local_addr)

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed


def create_protocol_pair(timeout_ms, dropper=None):
    received_a = []
    received_b = []

    addr_a = ("127.0.0.1", 9001)
    addr_b = ("127.0.0.1", 9002)

    proto_a = ReliableUDPProtocol(
        on_message=lambda msg, _addr: received_a.append(msg),
        timeout_ms=timeout_ms,
        debug=False,
    )
    proto_b = ReliableUDPProtocol(
        on_message=lambda msg, _addr: received_b.append(msg),
        timeout_ms=timeout_ms,
        debug=False,
    )

    transport_a = InMemoryTransport("A", proto_b, addr_a, dropper=dropper)
    transport_b = InMemoryTransport("B", proto_a, addr_b, dropper=dropper)

    proto_a.connection_made(transport_a)
    proto_b.connection_made(transport_b)

    proto_a.remote_addr = addr_b
    proto_b.remote_addr = addr_a

    return proto_a, proto_b, transport_a, transport_b, received_a, received_b


@pytest.mark.asyncio
async def test_retransmit_when_first_data_packet_is_lost():
    dropped = {"done": False}

    def drop_first_data_from_a(name, data):
        parsed = parse_packet(data)
        if parsed is None:
            return False

        pkt_type, seq, _checksum, _payload, _valid = parsed
        if name == "A" and pkt_type == PKT_DATA and seq == 0 and not dropped["done"]:
            dropped["done"] = True
            return True
        return False

    proto_a, _proto_b, transport_a, _transport_b, _recv_a, recv_b = create_protocol_pair(
        timeout_ms=30, dropper=drop_first_data_from_a
    )

    await asyncio.wait_for(proto_a.send_reliable("ola"), timeout=1.0)

    # Entrega única no recetor (sem perdas finais) apesar da perda inicial.
    assert recv_b == ["ola"]

    sent_data_packets = [
        p for p in transport_a.sent_packets if parse_packet(p)[0] == PKT_DATA  # type: ignore[index]
    ]
    # Fiabilidade: deve existir retransmissão (mais de 1 envio do mesmo DATA).
    assert len(sent_data_packets) >= 2


@pytest.mark.asyncio
async def test_duplicate_data_is_discarded_but_acked_again():
    dropped = {"done": False}

    def drop_first_ack_from_b(name, data):
        parsed = parse_packet(data)
        if parsed is None:
            return False

        pkt_type, seq, _checksum, _payload, _valid = parsed
        if name == "B" and pkt_type == PKT_ACK and seq == 0 and not dropped["done"]:
            dropped["done"] = True
            return True
        return False

    proto_a, _proto_b, transport_a, transport_b, _recv_a, recv_b = create_protocol_pair(
        timeout_ms=30, dropper=drop_first_ack_from_b
    )

    await asyncio.wait_for(proto_a.send_reliable("mensagem"), timeout=1.0)

    # Fiabilidade: pacote duplicado no recetor não pode ser entregue duas vezes.
    assert recv_b == ["mensagem"]

    sent_data_packets = [
        p for p in transport_a.sent_packets if parse_packet(p)[0] == PKT_DATA  # type: ignore[index]
    ]
    sent_ack_packets = [
        p for p in transport_b.sent_packets if parse_packet(p)[0] == PKT_ACK  # type: ignore[index]
    ]

    assert len(sent_data_packets) >= 2
    # ACK reenviado para o duplicado recebido.
    assert len(sent_ack_packets) >= 2
