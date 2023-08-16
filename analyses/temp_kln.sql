,
    referral_dependent_metrics AS (
        SELECT
            visit_id,
            COUNT(
                DISTINCT CASE WHEN referral_status = 'Yes' 
                THEN referral_id
                ELSE NULL END
                ) AS num_referrals,
            COUNT(
                DISTINCT CASE WHEN referral_status = 'Yes' AND referral_followed_up = 'Yes' 
                THEN referral_id
                ELSE NULL END
                ) AS num_followed_up_referrals
        FROM mwra_referrals
        GROUP BY visit_id
    )

SELECT *
FROM join_co_visit_dimensions_to_all_visits







LEFT JOIN referral_dependent_metrics USING (visit_id);