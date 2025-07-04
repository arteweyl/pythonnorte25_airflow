  SELECT
    account_id,
    SUM(
      CASE
         WHEN in_or_out = 'pix_in' THEN pix_amount
         WHEN in_or_out = 'pix_out' THEN -pix_amount
         ELSE 0 
      END
    ) AS total_amount
  FROM pix_movements
  WHERE 1=1
  AND status = 'completed'
  AND EPOCH_MS(pix_completed_at::BIGINT * 1000)::DATE >= '2020-01-01'
  AND EPOCH_MS(pix_completed_at::BIGINT * 1000)::DATE < '2021-01-01'
  GROUP BY account_id