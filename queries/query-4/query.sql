SELECT
  CAST((
    CASE
      WHEN MET.pt < 0 THEN 0
      WHEN MET.pt > 2000 THEN 2000
      ELSE MET.pt
    END - 10) / 20 AS BIGINT) * 20 + 10 AS x,
  COUNT(*) AS y
FROM {input_table}
WHERE cardinality(filter(Jet, x -> x.pt > 40)) > 1
GROUP BY CAST((
    CASE
      WHEN MET.pt < 0 THEN 0
      WHEN MET.pt > 2000 THEN 2000
      ELSE MET.pt
    END - 10) / 20 AS BIGINT) * 20 + 10
ORDER BY x;
