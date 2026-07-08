create schema if not exists raw;
create schema if not exists analytics;

create table if not exists raw.orders (
    order_id text primary key,
    order_timestamp timestamptz not null,
    customer_id text not null,
    product text not null,
    category text not null,
    units integer not null,
    unit_price numeric(12,2) not null,
    discount_pct numeric(5,2) not null,
    shipping_cost numeric(12,2) not null,
    state text not null,
    loaded_at timestamptz not null default now()
);

create table if not exists analytics.daily_sales (
    order_date date not null,
    order_count integer not null,
    gross_revenue numeric(14,2) not null,
    discount_amount numeric(14,2) not null,
    net_revenue numeric(14,2) not null,
    shipping_total numeric(14,2) not null,
    avg_order_value numeric(14,2) not null,
    updated_at timestamptz not null default now(),
    primary key (order_date)
);

create table if not exists analytics.category_sales (
    category text not null,
    order_count integer not null,
    units_sold integer not null,
    net_revenue numeric(14,2) not null,
    updated_at timestamptz not null default now(),
    primary key (category)
);
