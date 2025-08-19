--drop table audit_log
CREATE TABLE audit_log (

    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    article_number varchar(15) null,
    client_ip VARCHAR(105) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER NOT NULL,
    device_info TEXT,
    remote_addr VARCHAR(45),
    user_agent TEXT,
    user_id VARCHAR(15),
    city VARCHAR(255),
    country VARCHAR(255),
    isp VARCHAR(255),
    time_start TIMESTAMP NOT NULL,
    time_end TIMESTAMP NOT NULL,
    duration DOUBLE PRECISION NOT NULL,
    hitcount BIGSERIAL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);