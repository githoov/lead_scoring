SELECT lead.id AS lead_id
  , meeting
  , opportunity
  , founder
  , president
  , cto
  , ceo
  , cfo
  , coo
  , cio
  , cmo
  , vp
  , director
  , data_scientict
  , analyst
  , engineer_developer
  , consultant
  , product
  , architect
  , manager
  , technology
  , business_intelligence
  , department
  , inbound
  , inbound_form
  , original_referrer
  , search_phrase
  , source_type  
  , content_downloaded
  , campaign_touch.campaign_touches
  , campaign_touch.first_campaign
  , campaign_touch.last_campaign        
  , ROW_NUMBER() OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
                      ORDER BY created_date) AS nth_contact
  , FIRST_VALUE(postal_code IGNORE NULLS) 
      OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
           ORDER BY created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS postal_code  
  , FIRST_VALUE(state IGNORE NULLS) 
      OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
           ORDER BY created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state
  , FIRST_VALUE(country IGNORE NULLS) 
      OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
           ORDER BY created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS country
  , FIRST_VALUE(company__type___c IGNORE NULLS) 
      OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
           ORDER BY created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS company_type
  , FIRST_VALUE(number__of__employees___c IGNORE NULLS) 
      OVER(PARTITION BY MD5(REGEXP_REPLACE(TRIM(BOTH ' ' FROM REGEXP_REPLACE(LOWER(company), '(\.|,|\s)(inc|ltd|llc|llp|incorporated)+[^a-zA-Z]*$', '')), '[[:punct:]]', ''))
           ORDER BY created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS number_of_employees
FROM (SELECT id
        , CASE 
            WHEN intro__meeting___c 
            THEN 1 
            ELSE 0 
          END AS meeting
        , CASE 
            WHEN converted_opportunity_id IS NOT NULL 
            THEN 1 
            ELSE 0 
          END AS opportunity   
        , company
        , company__type___c 
        , number__of__employees___c
        , created_date
        , (title ~ '(f|F)ounder')::INT AS founder
        , (title ~ '^([^ice|VP])*(p|P)resident')::INT AS president
        , (title ~ '(CTO|cto)' OR title ~ '(t|T)echnical (o|O)fficer')::INT AS cto
        , (title ~ '(CEO|ceo)' OR title ~ '(e|E)xecutive (o|O)fficer')::INT AS ceo
        , (title ~ '(CFO|cfo)' OR title ~ '(f|F)inancial (o|O)fficer')::INT AS cfo
        , (title ~ '(COO|coo)' OR title ~ '(o|O)perational (o|O)fficer')::INT AS coo
        , (title ~ '(CIO|cio)' OR title ~ '(i|I)nformation (o|O)fficer')::INT AS cio
        , (title ~ '(CMO|cmo)' OR title ~ '(m|M)arketing (o|O)fficer')::INT AS cmo
        , (title ~ '(VP|vp)' OR title ~ '(v|V)ice (p|P)res')::INT AS vp
        , (LOWER(title) ~ 'director')::INT AS director
        , (LOWER(title) ~ 'scientist')::INT AS data_scientict
        , (LOWER(title) ~ 'analyst')::INT AS analyst
        , (LOWER(title) ~ '(engineer|developer)')::INT AS engineer_developer
        , (LOWER(title) ~ 'consultant')::INT AS consultant
        , (LOWER(title) ~ 'product')::INT AS product
        , (LOWER(title) ~ 'architect')::INT AS architect
        , (LOWER(title) ~ 'manager')::INT AS manager
        , (LOWER(title) ~ 'technology')::INT AS technology
        , (LOWER(title) ~ 'intelligence')::INT AS business_intelligence
        , LOWER(department___c) AS department 
        , CASE 
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%united states%'
              OR LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%usa%'
            THEN 'us'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%united kingdom%'
            THEN 'uk'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%canada%'
              OR LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%can'
            THEN 'ca'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%india%'
            THEN 'in'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%israel%'
            THEN 'il'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%france%'
            THEN 'fr'
            WHEN LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%brazil%'
            THEN 'br'  
            WHEN COALESCE(country, mkto_2____inferred__country___c, bizible___country___c) IS NULL
            THEN NULL
            ELSE LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c))
          END AS country
        , CASE
            WHEN state ~ '^[a-zA-Z]{2}$'
            THEN LOWER(state)
            WHEN mkto_2____inferred__state__region___c ~ '^[a-zA-Z]{2}$'
            THEN LOWER(mkto_2____inferred__state__region___c)
            WHEN bizible___region___c ~ '^[a-zA-Z]{2}$'
            THEN LOWER(bizible___region___c)      
            ELSE NULL
          END AS state
        , CASE
            WHEN postal_code ~ '^[0-9]{5}$'
            THEN postal_code
            WHEN mkto_2____inferred__postal__code___c ~ '^[0-9]{5}$'
            THEN mkto_2____inferred__postal__code___c
            WHEN COALESCE(postal_code, mkto_2____inferred__postal__code___c) ~ '[0-9]{5}-[0-9]{3}'
            THEN LEFT(COALESCE(postal_code, mkto_2____inferred__postal__code___c), 5)
            WHEN (LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%united states%'
              OR LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) LIKE '%usa%'
              OR LOWER(COALESCE(country, mkto_2____inferred__country___c, bizible___country___c)) = 'us')
              AND COALESCE(postal_code, mkto_2____inferred__postal__code___c) ~ '^[0-9]{4}$'
            THEN '0' || COALESCE(postal_code, mkto_2____inferred__postal__code___c)
            WHEN COALESCE(postal_code, mkto_2____inferred__postal__code___c) ~ '^[0-9a-zA-Z]{2,4}[[:space:]][0-9a-zA-Z]{3}$'
            THEN UPPER(REGEXP_SUBSTR(COALESCE(postal_code, mkto_2____inferred__postal__code___c), '^[0-9a-zA-Z]{2,4}[[:space:]][0-9a-zA-Z]{3}$'))
            ELSE NULL
          END AS postal_code
        , CASE
            WHEN grouping___c = 'Inbound'
            THEN 1
            ELSE 0
          END AS inbound
        , CASE 
            WHEN inbound__form__fillout___c 
            THEN 1 
            ELSE 0 
          END AS inbound_form
        , CASE
            WHEN mkto_2____original__referrer___c IS NULL
            THEN 'Unknown'
            WHEN mkto_2____original__referrer___c ~ 'google'
            THEN 'Google'
            WHEN mkto_2____original__referrer___c ~ 'looker'
            THEN 'Looker'
            WHEN mkto_2____original__referrer___c ~ 'quora'
            THEN 'Quora'
            WHEN mkto_2____original__referrer___c ~ 'aws'
            THEN 'AWS'
            WHEN mkto_2____original__referrer___c ~ 'linkedin'
            THEN 'LinkedIn'
            WHEN mkto_2____original__referrer___c ~ 'techcrunch'
            THEN 'Techcrunch'
            WHEN mkto_2____original__referrer___c ~ 'snowplowanalytics'
            THEN 'Snowplow'
            WHEN mkto_2____original__referrer___c ~ 'getapp'
            THEN 'Getapp'      
            ELSE 'Other'
          END AS original_referrer
        , CASE
            WHEN mkto_2____original__search__phrase___c IS NOT NULL 
            THEN 1
            ELSE 0
          END AS search_phrase
        , mkto_2____original__source__type___c AS source_type        
        , CASE
            WHEN content__download___c
            THEN 1
            ELSE 0
          END AS content_downloaded
      FROM salesforce._lead
      WHERE NOT is_deleted) AS lead
      LEFT JOIN ( SELECT lead_id
                    , COUNT(*) AS campaign_touches
                    , MIN(first_campaign) AS first_campaign
                    , MAX(last_campaign) AS last_campaign
                  FROM (SELECT cm.id
                          , cm.lead_id
                          , FIRST_VALUE(c.name IGNORE NULLS) OVER(PARTITION BY cm.lead_id ORDER BY cm.created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_campaign
                          , LAST_VALUE(c.name IGNORE NULLS) OVER(PARTITION BY cm.lead_id ORDER BY cm.created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_campaign
                        FROM salesforce._campaign_member AS cm
                        INNER JOIN salesforce._campaign AS c
                        ON cm.campaign_id = c.id
                        ) AS campaign_member
                  GROUP BY 1) AS campaign_touch
      ON lead.id = campaign_touch.lead_id
WHERE ((created_date) >= (CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', DATEADD(month,-15, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))) ))) 
AND (created_date) < (CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', DATEADD(month,12, DATEADD(month,-15, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))) ) ))))
