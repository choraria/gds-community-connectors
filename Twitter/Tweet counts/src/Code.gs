/**
 * Mandatory function required by Google Data Studio that should
 * return the authentication method required by the connector
 * to authorize the third-party service.
 * @return {Object} AuthType
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl('https://developer.twitter.com/en/docs/authentication/oauth-2-0/application-only')
    .build();
}

/**
 * Mandatory function required by Google Data Studio that should
 * clear user credentials for the third-party service.
 * This function does not accept any arguments and
 * the response is empty.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Mandatory function required by Google Data Studio that should
 * determine if the authentication for the third-party service is valid.
 * @return {Boolean}
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  return checkForValidKey(key);
}

/**
 * Mandatory function required by Google Data Studio that should
 * set the credentials after the user enters either their
 * credential information on the community connector configuration page.
 * @param {Object} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var key = request.key;
  var validKey = checkForValidKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the user configurable options for the connector.
 * @param {Object} request
 * @return {Object} fields
 */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  config
    .newTextInput()
    .setId("query")
    .setName("Query")
    .setHelpText('Check Twitter\'s query builder here — https://developer.twitter.com/apitools/query?query=')
    .setPlaceholder("#bitcoin OR Bitcoin")
    .setAllowOverride(true);

  config
    .newSelectSingle()
    .setId('granularity')
    .setName('Granularity')
    .setHelpText('You can requeset \'minute\', \'hour\', or \'day\' granularity. The default granularity, if not specified is \'hour\'.')
    .setAllowOverride(true)
    .addOption(config.newOptionBuilder().setLabel('Minute').setValue('minute'))
    .addOption(config.newOptionBuilder().setLabel('Hour').setValue('hour'))
    .addOption(config.newOptionBuilder().setLabel('Day').setValue('day'))

  return config.build();
}

/**
 * Supports the getSchema() function
 * @param {Object} request
 * @return {Object} fields
 */
function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newDimension()
    .setId('end')
    .setType(types.YEAR_MONTH_DAY_SECOND)

  fields.newMetric()
    .setId('tweet_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  return fields;
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the schema for the given request.
 * This provides the information about how the connector's data is organized.
 * @param {Object} request
 * @return {Object} fields
 */
function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

/**
 * Takes the requested fields with the API response and
 * return rows formatted for Google Data Studio.
 * @param {Object} requestedFields
 * @param {Object} response
 * @return {Array} values
 */
function responseToRows(requestedFields, response) {
  return response.map(function (day) {
    var row = [];
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
        case 'end':
          return row.push(Utilities.formatDate(new Date(day.end), "GMT", "yyyyMMddHHmmss"));
        case 'tweet_count':
          return row.push(day.tweet_count);
        default:
          return row.push('');
      }
    });
    return { values: row };
  });
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the tabular data for the given request.
 * @param {Object} request
 * @return {Object}
 */
function getData(request) {
  var requestConfigParams = request.configParams;
  var query = requestConfigParams.query;
  var granularity = requestConfigParams.granularity || 'hour';
  var requestedFieldIds = request.fields.map(function (field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);
  var userProperties = PropertiesService.getUserProperties();
  var token = userProperties.getProperty('dscc.key');
  let nextToken;
  let rowData = [];
  let userError = false;
  let response;

  do {
    var baseURL = `https://api.twitter.com/2/tweets/counts/recent?granularity=${granularity}&query=${encodeURIComponent(query)}`;
    if (nextToken) {
      baseURL += `&next_token=${nextToken}`;
    }
    var options = {
      'method': 'GET',
      'headers': {
        'Authorization': 'Bearer ' + token,
      },
      'muteHttpExceptions': true
    };
    response = UrlFetchApp.fetch(baseURL, options);
    if (response.getResponseCode() == 200) {
      var parsedResponse = JSON.parse(response.getContentText());
      parsedResponse.data.forEach(day => rowData.push({
        "end": day.end,
        "tweet_count": day.tweet_count
      }))
      nextToken = parsedResponse.meta?.next_token;
    } else {
      userError = true;
      break;
    }
  } while (nextToken);

  if (userError) {
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + response)
      .setText('Error fetching data from API. Exception details: ' + response)
      .throwException();
  } else {
    var rows = responseToRows(requestedFields, rowData);
    return {
      schema: requestedFields.build(),
      rows: rows
    };
  }
}

/**
 * Checks if the Key/Token provided by the user is valid
 * @param {String} key
 * @return {Boolean}
 */
function checkForValidKey(key) {
  var token = key;
  var baseURL = 'https://api.twitter.com/2/tweets/counts/recent?granularity=day&query=google';
  var options = {
    'method': 'GET',
    'headers': {
      'Authorization': 'Bearer ' + token,
    },
    'muteHttpExceptions': true
  };
  var response = UrlFetchApp.fetch(baseURL, options);
  if (response.getResponseCode() == 200) {
    return true;
  } else {
    return false;
  }
}
