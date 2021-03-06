WITH temp AS (
  SELECT event, MET.sumet, COUNT(*)
  FROM {input_table}
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m1(m1, idx1)
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m2(m2, idx2)
  WHERE cardinality(Muon) > 1 AND idx1 < idx2 AND m1.charge <> m2.charge AND
    SQRT(2 * m1.pt * m2.pt * (COSH(m1.eta - m2.eta) - COS(m1.phi - m2.phi))) > 60 AND
    SQRT(2 * m1.pt * m2.pt * (COSH(m1.eta - m2.eta) - COS(m1.phi - m2.phi))) < 120
  GROUP BY event, MET.sumet
  HAVING COUNT(*) > 0
)
SELECT
  CAST((
    CASE
      WHEN sumet < 0 THEN 0
      WHEN sumet > 2000 THEN 2000
      ELSE sumet
    END - 10) / 20 AS BIGINT) * 20 + 10 AS x,
  COUNT(*) AS y
FROM temp
GROUP BY CAST((
    CASE
      WHEN sumet < 0 THEN 0
      WHEN sumet > 2000 THEN 2000
      ELSE sumet
    END - 10) / 20 AS BIGINT) * 20 + 10
ORDER BY x;
