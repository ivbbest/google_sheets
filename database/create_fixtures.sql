CREATE TABLE IF NOT EXISTS sheet (
    id serial PRIMARY KEY,
    id_number INT NOT NULL,
    order_number INT UNIQUE NOT NULL,
    price INT NOT NULL,
    price_rub INT NOT NULL,
    date DATE NOT NULL
);