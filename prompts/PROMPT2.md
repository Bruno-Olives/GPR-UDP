Plano prático (agora)

Validar que tudo corre no Raspberry.
Fazer testes base sem perda de rede.
Repetir com perda/latência simulada.
Registar métricas e comparar UDP Fiável vs TCP.

1. Teste rápido funcional
   Em 2 terminais no Raspberry (ou 2 máquinas diferentes), corre o chat UDP fiável de reliable_udp_chat.py:

Terminal A
python3 reliable_udp_chat.py --local-port 9001 --remote-host 127.0.0.1 --remote-port 9002

Terminal B
python3 reliable_udp_chat.py --local-port 9002 --remote-host 127.0.0.1 --remote-port 9001

Depois envia mensagens e confirma logs:

Enviando Seq X
ACK Recebido X
Retransmitindo Seq X (só deve aparecer quando há perda)
Faz o mesmo com TCP em tcp_chat.py:
Terminal A
python3 tcp_chat.py --local-port 9101 --remote-host 127.0.0.1 --remote-port 9102

Terminal B
python3 tcp_chat.py --local-port 9102 --remote-host 127.0.0.1 --remote-port 9101

2. Correr testes automáticos no Raspberry
   python3 -m pytest -q test_protocol.py

Se passar, o protocolo está ok em lógica de retransmissão/duplicados.

3. Medição base (sem perturbação)
   Faz 3 execuções UDP e 3 execuções TCP, com o mesmo número de mensagens e tamanho.
   Regista:

Latência média por mensagem
Mensagens por segundo
Número de retransmissões (UDP)
CPU (%)

4. Simular rede “má” para comparação real
   No Linux (Raspberry), podes usar netem. Exemplo no interface wlan0:

sudo tc qdisc add dev wlan0 root netem loss 5% delay 50ms 10ms

Repete os testes UDP e TCP com exatamente a mesma carga.
No fim remove:
sudo tc qdisc del dev wlan0 root

5. Tabela final para o relatório
   Cria uma tabela com colunas:

Protocolo
Cenário (normal, perda 5%, atraso 50ms)
Latência média
Throughput
Retransmissões
Observações
O que fazer já a seguir (curto)

Corre primeiro o UDP fiável em 2 terminais e confirma logs.
Corre o TCP com a mesma carga.
Faz uma primeira tabela base (sem netem).
Se quiseres, eu ajudo-te a criar um script de benchmark automático para gerar resultados em CSV.
