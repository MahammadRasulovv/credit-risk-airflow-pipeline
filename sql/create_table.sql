CREATE TABLE IF NOT EXISTS credit_applications (
    application_id    INT PRIMARY KEY,
    customer_id       VARCHAR(10),
    customer_name     VARCHAR(100),
    requested_amount  NUMERIC(12,2),
    monthly_income    NUMERIC(12,2),
    existing_debt     NUMERIC(12,2),
    employment_years  NUMERIC(4,1),
    age               INT,
    application_date  DATE
);
