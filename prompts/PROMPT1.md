Implementação concluída com os 3 ficheiros pedidos:

reliable_udp_chat.py
tcp_chat.py
test_protocol.py
O que foi entregue:

Em reliable_udp_chat.py:

Classe ReliableUDPProtocol a herdar de asyncio.DatagramProtocol.
Cabeçalho binário com struct.pack: Type, Sequence Number, Checksum.
Checksum CRC32 para validação de integridade.
ARQ Stop-and-Wait com timeout e retransmissão automática.
Lógica de recetor com ACK por pacote e descarte de duplicados/out-of-order.
Leitura de stdin sem bloquear receção UDP (run_in_executor).
Logs de debug pedidos:
Enviando Seq X
ACK Recebido X
Retransmitindo Seq X
Comentários explícitos nos pontos de controlo de fiabilidade.
Em tcp_chat.py:

Script TCP nativo de comparação usando asyncio.start_server e asyncio.open_connection.
Chat P2P simples com leitura não bloqueante de teclado.
Em test_protocol.py:

Testes com pytest e pytest-asyncio.
Simulação de perda de pacote por código (transporte em memória com dropper).
Verificação de retransmissão quando DATA é perdido.
Verificação de descarte de duplicados quando ACK é perdido.
Validação executada:

Instalei dependências de teste: pytest e pytest-asyncio.
Resultado dos testes: 2 passed in 0.13s.
Se quiseres, no próximo passo posso acrescentar métricas automáticas (latência média, retransmissões, throughput) para facilitar a comparação UDP Fiável vs TCP no Raspberry Pi.
