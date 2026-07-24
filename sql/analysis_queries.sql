SELECT * FROM ecommerce.pipeline_runs ORDER BY finished_at DESC LIMIT 5;
SELECT * FROM ecommerce.data_quality_results ORDER BY batch_id, check_name;
SELECT * FROM ecommerce.mart_country_sales ORDER BY net_revenue DESC;
SELECT * FROM ecommerce.mart_customer_rfm ORDER BY monetary DESC LIMIT 20;
SELECT * FROM ecommerce.mart_product_performance ORDER BY net_revenue DESC LIMIT 20;
