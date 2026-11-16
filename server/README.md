# Serviço persistente de cotações MendtradeR

Este serviço mantém uma ligação própria à API pública de cotações da Deriv, independentemente de o utilizador ter a página aberta. Não usa token de conta e **não executa ordens**.

## Publicação

1. Cria um serviço Node.js num alojamento que mantenha processos sempre ativos (por exemplo, Render Web Service pago ou VPS).
2. Liga o repositório `mendi2002/MendtradeR-v5`.
3. Define a pasta raiz como `server`.
4. Comando de instalação: `npm install`.
5. Comando de arranque: `npm start`.
6. Define a variável `DEFAULT_SYMBOL` como `R_100` (ou outro símbolo Deriv válido).
7. Opcional: define `ALLOWED_ORIGINS` com o domínio público da MendtradeR. Se ficar vazio, não há filtro de origem; usa HTTPS/WSS no alojamento.
8. Depois da publicação, confirma que `https://TEU-SERVICO/health` responde com JSON `{"ok":true,...}`.

O serviço abre automaticamente o fluxo do símbolo padrão quando inicia e tenta reconectar se a Deriv interromper a ligação. Os navegadores ligam-se a `wss://TEU-SERVICO/stream?symbol=R_100`.

## Variáveis

- `PORT`: fornecida pelo alojamento.
- `DEFAULT_SYMBOL`: símbolo inicial, por omissão `R_100`.
- `ALLOWED_ORIGINS`: lista de origens separadas por vírgulas.
- `MAX_CLIENTS`: máximo de clientes WebSocket, por omissão 50.
- `MAX_SYMBOLS`: máximo de símbolos simultâneos, por omissão 5.

## Limites importantes

- A fonte é o feed público de ticks da Deriv; não inclui saldo, contratos, resultados de operações ou autorização de negociação.
- O processo tem de ficar alojado num plano que não suspenda o serviço por inatividade. Planos gratuitos que adormecem não garantem recolha contínua.
- A disponibilidade depende do alojamento, rede e API da Deriv. Verifica o estado em `/health` e nos logs do serviço.
- Não coloques tokens privados da Deriv neste serviço. A integração de ordens exige uma arquitetura separada, autenticação segura e testes específicos.
