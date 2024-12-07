-- This script can be referenced when reading medium blog series:
-- [Blog 1](https://medium.com/google-cloud/gemini-in-bigquery-a-comprehensive-guide-to-multimodal-data-analysis-part-1-d7a9d246080e)
-- [Blog 2](https://medium.com/@charulatashelar/gemini-in-bigquery-unlocking-multimodal-search-with-vector-embeddings-part-2-69e26c36fee5)

-- Create the object table. This table will store the links to product images in GCS.
CREATE OR REPLACE EXTERNAL TABLE `bqml_mm_search.product_images`
WITH CONNECTION `us.bqml_vertex_ai_connection`
OPTIONS
  ( object_metadata = 'SIMPLE',
    uris = ['gs://mm_product_search/product_images/*']
  );

-- Create the remote model. This model will be used to generate image embeddings.
CREATE OR REPLACE MODEL `bqml_mm_search.multimodal_embedding_model`
  REMOTE WITH CONNECTION `us.bqml_vertex_ai_connection`
  OPTIONS (ENDPOINT = 'multimodalembedding@001');

  -- Generate image embeddings. This will generate embeddings for each image in the product_images table.
CREATE OR REPLACE TABLE `bqml_mm_search.product_image_embeddings`
AS
SELECT *
FROM
  ML.GENERATE_EMBEDDING(
    MODEL `bqml_mm_search.multimodal_embedding_model`,
    (SELECT * FROM `bqml_mm_search.product_images` WHERE content_type = 'image/jpeg' LIMIT 10000))

-- See if there were any embedding generation failures.
SELECT DISTINCT(ml_generate_embedding_status),
  COUNT(uri) AS num_rows
FROM bqml_mm_search.product_image_embeddings
GROUP BY 1;

-- Create a vector index. This will help to speed up the search process.
CREATE OR REPLACE
  VECTOR INDEX `met_images_index`
ON
  bqml_mm_search.product_image_embeddings(ml_generate_embedding_result)
  OPTIONS (
    index_type = 'IVF',
    distance_type = 'COSINE');

-- Check if the vector index has been created.
SELECT table_name, index_name, index_status,
  coverage_percentage, last_refresh_time, disable_reason
FROM bqml_mm_search.INFORMATION_SCHEMA.VECTOR_INDEXES
WHERE index_name = 'met_images_index';

--------------------------------------------
-------------- Text to Image Search ----------------
--------------------------------------------

-- Generate an embedding for the search text.
CREATE OR REPLACE TABLE `bqml_mm_search.search_embedding`
AS
SELECT * FROM ML.GENERATE_EMBEDDING(
  MODEL `bqml_mm_search.multimodal_embedding_model`,
  (
    SELECT 'black t-shirt' AS content
  )
);

-- Perform a cross-modality text-to-image search. This will search for images that are similar to the search text.
CREATE OR REPLACE TABLE `bqml_mm_search.vector_search_results` AS
SELECT base.uri AS gcs_uri, distance
FROM
  VECTOR_SEARCH(
    TABLE `bqml_mm_search.product_image_embeddings`,
    'ml_generate_embedding_result',
    TABLE `bqml_mm_search.search_embedding`,
    'ml_generate_embedding_result',
    top_k => 5);

-- Display the search result.
select * from  `bqml_mm_search.vector_search_results` 

--------------------------------------------
-------------- Image to Image Search ----------------
--------------------------------------------

-- Create the object table. This table will store the search product images.
CREATE OR REPLACE EXTERNAL TABLE `bqml_mm_search.search_product_images`
WITH CONNECTION `us.bqml_vertex_ai_connection`
OPTIONS
  ( object_metadata = 'SIMPLE',
    uris = ['gs://mm_product_search/test_images/*']
  );

-- Generate image embeddings. This will generate embeddings for each image in the search_product_images table.
CREATE OR REPLACE TABLE `bqml_mm_search.search_product_image_embeddings`
AS
SELECT *
FROM
  ML.GENERATE_EMBEDDING(
    MODEL `bqml_mm_search.multimodal_embedding_model`,
    (SELECT * FROM `bqml_mm_search.search_product_images` WHERE content_type = 'image/jpeg' LIMIT 1000))

-- See if there were any embedding generation failures.
SELECT DISTINCT(ml_generate_embedding_status),
  COUNT(uri) AS num_rows
FROM bqml_mm_search.search_product_image_embeddings
GROUP BY 1;

-- Perform a cross-modality image-to-image search. This will search for images that are similar to the search image.
CREATE OR REPLACE TABLE `bqml_mm_search.vector_image_search_results` AS
SELECT query.uri, base.uri AS gcs_uri, distance
FROM
  VECTOR_SEARCH(
    TABLE `bqml_mm_search.product_image_embeddings`,
    'ml_generate_embedding_result',
    -- TABLE `bqml_mm_search.search_product_image_embeddings`,
    (SELECT * FROM `bqml_mm_search.search_product_image_embeddings` where uri like '%gs://mm_product_search/test_images/test 1.jpg%'),
    'ml_generate_embedding_result',
    top_k => 5
    );

-- Display the search result.
select * from  `bqml_mm_search.vector_image_search_results`
order by distance desc
