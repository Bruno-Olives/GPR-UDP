Aqui vai o Passo 3 (medição base, sem perturbação) de forma simples e prática.

Objetivo: medir latência média e throughput, 3 vezes para UDP e 3 vezes para TCP, com o mesmo número de mensagens e tamanho.

1. Criar um ficheiro de mensagens (ex: 1000 linhas ~100 bytes)

```
python3 - <<'PY' > messages.txt
for i in range(1000):
    print(f"msg {i:04d} " + "X"*90)
PY
```

2. UDP fiável – receptor
   No lado que vai receber:

```
python3 reliable_udp_chat.py --local-port 9002 --remote-host IP_DO_SENDER --remote-port 9001 > udp_recv.log
```

3. UDP fiável – emissor (medir tempo)
   No lado que vai enviar:

```
time python3 reliable_udp_chat.py --local-port 9001 --remote-host IP_DO_RECETOR --remote-port 9002 < messages.txt
```

4. Calcular métricas

```
wc -l messages.txt
```

Throughput (mensagens/s) = número de mensagens / tempo total
Latência média (ms) ≈ (tempo total / número de mensagens) × 1000 5) Repetir 3 vezes
Faz 3 runs idênticas e tira a média.

6. Repetir o mesmo para TCP
   Usa o mesmo messages.txt, mas com tcp_chat.py:

Receptor:

```
python3 tcp_chat.py --local-port 9102 --remote-host IP_DO_SENDER --remote-port 9101 > tcp_recv.log
```

Emissor:

```
time python3 tcp_chat.py --local-port 9101 --remote-host IP_DO_RECETOR --remote-port 9102 < messages.txt
```

Opcional (CPU)
Durante o teste, num outro terminal:

```
ps -C python3 -o pid,%cpu,%mem,cmd
```
