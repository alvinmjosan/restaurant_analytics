-- Create or refresh a Silver streaming table for reviews
CREATE OR REFRESH STREAMING TABLE `02_silver`.fact_reviews(
    -- Keep only reviews with valid sentiment values
    CONSTRAINT valid_sentiment EXPECT(sentiment IN ('positive', 'neutral', 'negative')) ON VIOLATION DROP ROW,
    -- Drop reviews where rating is 0 or negative
    CONSTRAINT valid_rating EXPECT(rating > 0) ON VIOLATION DROP ROW
)
AS
-- Read reviews from Bronze and send each review to the AI model
WITH model_response AS (
    SELECT *,
        ai_query(
            -- AI model used to analyze the review
            'databricks-gpt-oss-20b',
            -- Build the prompt sent to the AI model
            CONCAT(
                -- Tell the AI exactly what JSON format to return
                'Analyze the following review and return ONLY a valid JSON object with this exact structure: ',
                -- Ask the AI to identify sentiment
                '{"sentiment": "<positive/neutral/negative>", ',
                -- Ask whether the review has a delivery issue
                '"issue_delivery": <true/false>, ',
                -- Ask for the reason for a delivery issue
                '"issue_delivery_reason": "<reason or empty string>", ',
                -- Ask whether the review has a food quality issue
                '"issue_food_quality": <true/false>, ',
                -- Ask for the reason for a food quality issue
                '"issue_food_quality_reason": "<reason or empty string>", ',
                -- Ask whether the review has a pricing issue
                '"issue_pricing": <true/false>, ',
                -- Ask for the reason for a pricing issue
                '"issue_pricing_reason": "<reason or empty string>", ',
                -- Ask whether the review has a portion-size issue
                '"issue_portion_size": <true/false>, ',
                -- Ask for the reason for a portion-size issue
                '"issue_portion_size_reason": "<reason or empty string>"} ',
                -- Define rules the AI must follow
                'Rules: sentiment must be exactly one of: positive, neutral, negative. ',
                'Each issue field is true/false only. ',
                'Each reason field should contain a brief explanation if the issue is true, otherwise empty string. ',
                -- Add the actual review text to the prompt
                'Review text: ', review_text
            )
        ) AS analysis_json
    -- Read reviews continuously from the Bronze streaming table
    FROM STREAM `01_bronze`.reviews
)
-- Select the original review information and AI-generated analysis
SELECT
    review_id,
    order_id,
    customer_id,
    restaurant_id,
    rating,
    review_text,
    -- Keep the complete JSON response from the AI
    analysis_json,
    -- Extract sentiment from the AI-generated JSON
    get_json_object(analysis_json, '$.sentiment') AS sentiment,
    -- Extract delivery issue and convert it to Boolean
    get_json_object(analysis_json, '$.issue_delivery')::BOOLEAN AS issue_delivery,      -- $          → start from the root of the JSON object
                                                                                        -- .          → go inside a field
                                                                                        -- sentiment  → field you want
    -- Extract the reason for the delivery issue
    get_json_object(analysis_json, '$.issue_delivery_reason') AS issue_delivery_reason,
    -- Extract food quality issue
    get_json_object(analysis_json, '$.issue_food_quality')::BOOLEAN AS issue_food_quality,
    -- Extract food quality issue reason
    get_json_object(analysis_json, '$.issue_food_quality_reason') AS issue_food_quality_reason,
    -- Extract pricing issue and convert it to Boolean
    get_json_object(analysis_json, '$.issue_pricing')::BOOLEAN AS issue_pricing,
    -- Extract pricing issue reason
    get_json_object(analysis_json, '$.issue_pricing_reason') AS issue_pricing_reason,
    -- Extract portion-size issue and convert it to Boolean
    get_json_object(analysis_json, '$.issue_portion_size')::BOOLEAN AS issue_portion_size,
    -- Extract portion-size issue reason
    get_json_object(analysis_json, '$.issue_portion_size_reason') AS issue_portion_size_reason,
    review_timestamp
FROM model_response;