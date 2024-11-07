SELECT
    a.id_pedido,
    a.data_pedido,
    a.valor_pedido,
    b.evento AS ultimo_evento,
    b.data_evento
FROM
    tabela_a a
JOIN (
    SELECT
        id_pedido,
        evento,
        data_evento,
        ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY data_evento DESC) AS rn
    FROM
        tabela_b
) b ON a.id_pedido = b.id_pedido AND b.rn = 1
ORDER BY
    a.id_pedido;
