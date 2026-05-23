function fn() {

  var config = {
    baseUrl: 'http://localhost:8080/api/v1'
  };

  var DbUtils = Java.type('com.poc.CanonicalIngestionEngine.testutil.DbUtils');
  config.db = new DbUtils();

  return config;
}