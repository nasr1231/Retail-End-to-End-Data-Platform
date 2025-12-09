# Schema Registry & Avro Schema — producer

This README documents the Avro schema used by the producer and guidance for registering and using it with a Schema Registry.

## Overview
The SalesEvent Avro schema models events related to commerce transactions (orders, cancels, returns). It defines enums for controlled vocabularies, numeric and string fields for order and item data, timestamp fields, and defaults for monetary fields.

Top-level metadata
- namespace: com.commerce.sales.v1
- type: record
- name: SalesEvent

Important enums
- EventType: ORDER, CANCEL, RETURN
- CurrencyCode: USD, CAD, GBP, AUD, EUR
- PaymentMethod: CASH, WALLET, DEBIT_CARD, CREDIT_CARD (default: CASH)

Fields (high level)
- event_type — enum EventType
- order_number — long
- line_item — int
- order_serial — string
- order_date — string (marked with logicalType: timestamp-millis; see "Notes on logical types")
- delivery_date — union [null, long] with logicalType: timestamp-millis, default null
- customerkey, storekey, productkey — long
- quantity — int
- currency_code — enum CurrencyCode
- payment_method — enum PaymentMethod (default CASH)
- status — string
- order_group_id — string
- unit_price_usd — double (default 0.0)
- line_total_amount — double (default 0.0)
- event_timestamp — string (marked with logicalType: timestamp-millis; see "Notes on logical types")

Notes on logical types and schema correctness
- The schema uses "logicalType": "timestamp-millis" on fields typed as "string" (order_date and event_timestamp). Avro's standard logical type "timestamp-millis" is intended for a numeric long representing milliseconds since the epoch. Using "timestamp-millis" with a string type is non-standard and may cause validation/serialization problems with many Avro libraries.
  Recommendation:
  - If you want an integer epoch milliseconds, change the schema fields to:
    {"type": ["null","long"], "logicalType": "timestamp-millis", ...} or just {"type":"long", "logicalType":"timestamp-millis"} (with null union if optional).
  - If you want to store a formatted timestamp string, remove "logicalType" and keep type "string"; use explicit formatting (e.g., ISO 8601) in your producers and consumers.
- delivery_date is correctly declared as union with long + logicalType. Keep consistent logical type usage across fields.

Example Avro (JSON) instance
```json
{
  "event_type": "ORDER",
  "order_number": 123456789,
  "line_item": 1,
  "order_serial": "O-01-1",
  "order_date": "09-12-2025 15:30:00",
  "delivery_date": null,
  "customerkey": 1001,
  "storekey": 2002,
  "productkey": 3003,
  "quantity": 2,
  "currency_code": "USD",
  "payment_method": "CREDIT_CARD",
  "status": "COMPLETED",
  "unit_price_usd": 19.99,
  "line_total_amount": 39.98,
  "event_timestamp": "09-12-2025 15:30:01"
}
```

