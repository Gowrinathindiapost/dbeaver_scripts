-- mis_db.mv_booking_dom definition

CREATE TABLE mis_db.mv_booking_dom_test
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime,

    `destination_pincode` Int32,

    `delivery_location` String,

    `article_type` String,

    `tariff` Float32
)
ENGINE = Log;