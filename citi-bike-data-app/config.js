/*
 * Configuration File Setup
 * 1. Create a copy of this file named "config.js" in same directory
 * 2. Edit values in <>s in "config.js" corresponding to your snowflake demo
 *    instance
 */
var config = {
//  snowflake_account: 'test',
  snowflake_user: 'test',
  // note: this is only a generated test key, can be shared publicly
  snowflake_private_key: `-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQC4Rca7RU+O5oTE87fquFk0Ik7VYQJTBcQ84nauz//5OyH+ILXFsRHBaokMPOCONRAZrGbTZ7/sJ6OH/WYtKpELJKEXL5r35trHGmgGZDw6x6ht+CKXO+yt9HP8cQ2BVwdF/vRwhgTmAG4PLZwY3MP3DJ+oEpEgvWDp97n4hwmfsQIDAQABAoGAQFxOnxYERZsKAGhHcnnU3jqlYi2xzCGVetZ2MXSAqSIYK1RtaJEB7JYzK80PeVvbNbxbZbc091yY52SADYJyieOm2GjeIt7FwMM6EX6u/gINxEIfkTR/5+6m6xlaV/IWTlsHUgKTq+R+/ahqUsfuitCUycC92BwEib6YKW0aiAkCQQDqUCg/aIcdtPlFSv8U+8LFfgyrZfhdiLbmOi3oJWl93gHgKhNnpnshodXPVx7aP4zBwADBMwtP3cQJ2+b3wLIDAkEAyVPzy5jdqYjxiv/aUcYhnLUf1I5qhvbqvzxFIEasGPlN8nvAY2ZJI0F/OstTrf8pXlDUxknrtppp00XsGg4zOwJBAN9GtrOFdYt3UjkXd+6U4Uq0DHqfVoY6qp7EPc6DJ/0KuprTPV59o8OupUFIcVvjRsuxnIZ9j3/xgMcsRvE7K+UCQGQAgGOMDeaNMDVz+tSNjtqRGTtydjWN5nKRFGEA2bEZ/H7Ku3hkMUYC3ZitsGYIDVtc2SOZSi0MrC4WWD1k+ksCQQCFHz01BskO44wsrzDg7G6q8/ok+an4ayr1n3q0IrpV8JrANkm6VF9LltFvfnt2zDFe6+Ceb52Poqs4Cm5LJYmX
-----END RSA PRIVATE KEY-----`,
  snowflake_database: 'test',
  snowflake_schema: 'test',
  snowflake_warehouse: 'test',
  snowflake_url: 'http://snowflake.localhost.localstack.cloud:4566',
  snowflake_host: 'snowflake.localhost.localstack.cloud',
  pool_max: 200
}
module.exports = config;
