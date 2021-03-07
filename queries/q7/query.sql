-- Compute sum of pt of each matching jet
WITH matching_jets AS (
  SELECT event, SUM(j.pt) AS pt_sum
  FROM {input_table}
  CROSS JOIN UNNEST(Jet) AS _j(j)
  WHERE
    j.pt > 30 AND
    cardinality(
        filter(
            Electron,
            x -> x.pt > 10 AND
                 sqrt( (j.eta - x.eta) * (j.eta - x.eta) +
                       pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 0.4)) = 0 AND
    cardinality(
        filter(
            Muon,
            x -> x.pt > 10 AND
                 sqrt( (j.eta - x.eta) * (j.eta - x.eta) +
                       pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 0.4)) = 0
  GROUP BY event
)
-- Compute the histogram
SELECT
  CAST((
    CASE
      WHEN pt_sum < 15 THEN 15
      WHEN pt_sum > 200 THEN 200
      ELSE pt_sum
    END - 0.925) / 1.85 AS BIGINT) * 1.85 + 0.925 AS x,
  COUNT(*) AS y
FROM matching_jets
GROUP BY CAST((
    CASE
      WHEN pt_sum < 15 THEN 15
      WHEN pt_sum > 200 THEN 200
      ELSE pt_sum
    END - 0.925) / 1.85 AS BIGINT) * 1.85 + 0.925
ORDER BY x;
