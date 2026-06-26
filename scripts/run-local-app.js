#!/usr/bin/env node

process.env.DATABASE_URL ||= "postgres://autoinfo:autoinfo@127.0.0.1:55432/autoinfo";
process.env.DATABASE_OPTIONS ||= "-c jit=off";
process.env.PORT ||= "3002";
process.env.ENABLE_MOCK_DATA ||= "false";

require("../server");
