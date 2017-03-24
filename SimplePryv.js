// Example of Standard Connections in Web Data Connectors using JSONPlaceholder JSON endpoints
// Tableau 10.1 - WDC API v2.1



// Define our Web Data Connector
(function(){
  var myConnector = tableau.makeConnector();

  var pyConnection = null;


  // Define the schema
  myConnector.getSchema = function(schemaCallback) {


    var event_num_cols = [{
      id: "id",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "streamId",
      alias: "streamId",
      dataType: tableau.dataTypeEnum.string,
      foreignKey: {tableId: 'stream', columnId: 'id'}
    }, {
      id: "time",
      alias: "time",
      dataType: tableau.dataTypeEnum.datetime
    }, {
      id: "duration",
      alias: "duration",
      dataType: tableau.dataTypeEnum.float
    }, {
      id: "type",
      alias: "type",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "content",
      alias: "content",
      dataType: tableau.dataTypeEnum.float,
      columnRole: tableau.columnRoleEnum.measure
    }];

    var eventNumTable = {
      id: "eventNum",
      alias: "Numerical Events",
      columns: event_num_cols
    };


    var event_location_cols = [{
      id: "id",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "streamId",
      alias: "streamId",
      dataType: tableau.dataTypeEnum.string,
      foreignKey: {tableId: 'stream', columnId: 'id'}
    }, {
      id: "time",
      alias: "time",
      dataType: tableau.dataTypeEnum.datetime
    }, {
      id: "duration",
      alias: "duration",
      dataType: tableau.dataTypeEnum.float
    }, {
      id: "type",
      alias: "type",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "lat",
      alias: "latitude",
      dataType: tableau.dataTypeEnum.float
    }, {
      id: "lon",
      alias: "longitude",
      dataType: tableau.dataTypeEnum.float
    }];

    var eventLocationTable = {
      id: "eventLocation",
      alias: "Location Events",
      columns: event_location_cols
    };


    // Schema for time and URL data
    var stream_cols = [{
      id: "id",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "name",
      alias: "name",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "parentId",
      alias: "parentId",
      dataType: tableau.dataTypeEnum.string,
      foreignKey: {tableId: 'stream', columnId: 'id'}
    }];

    var streamTable = {
      id: "stream",
      alias: "Streams table",
      columns: stream_cols
    };
    schemaCallback([streamTable, eventNumTable, eventLocationTable]);
  };


  myConnector.getData = function(table, doneCallback) {
    // Load our data from the API. Multiple tables for WDC work by calling getData multiple times with a different id
    // so we want to make sure we are getting the correct table data per getData call
    switch (table.tableInfo.id) {
      case 'stream':
        getStreams(table, doneCallback);
        break;
      case 'eventNum':
        getNumEvents(table, doneCallback);
        break;
      case 'eventLocation':
        getLocationEvents(table, doneCallback);
        break;
    }
  }



  /**
   * @param {Pryv.Connection} connection
   * @param {string} langCode
   */
  function onSignedIn(connection, langCode) {
    console.log('Signed in!');
    pyConnection = connection;
    tableau.password = null;
    tableau.username = null;
    getPYConnection();
    $('#submitButton').show();
  }


  $(document).ready(function() {
    if (! tableau.password) {
      $('#submitButton').hide();
    }
  });


  // Init function for connector, called during every phase but
  // only called when running inside the simulator or tableau
  myConnector.init = function(initCallback) {
    tableau.authType = tableau.authTypeEnum.custom;

    getPYConnection();


    initCallback();



    // If we are in the auth phase we only want to show the UI needed for auth
    if (tableau.phase == tableau.phaseEnum.authPhase) {
      $("#submitButton").hide();
    }

    if (tableau.phase == tableau.phaseEnum.gatherDataPhase) {
      // If API that WDC is using has an enpoint that checks
      // the validity of an access token, that could be used here.
      // Then the WDC can call tableau.abortForAuth if that access token
      // is invalid.
    }

    initCallback();

    // If we are not in the data gathering phase, we want to store the token
    // This allows us to access the token in the data gathering phase
    if (tableau.phase == tableau.phaseEnum.interactivePhase || tableau.phase == tableau.phaseEnum.authPhase) {
      if (tableau.password) {
        $('#submitButton').show();
      } else {
        $('#submitButton').hide();
      }
    }
  };


  tableau.registerConnector(myConnector);



// Authenticate user

  var authSettings = {
    requestingAppId: 'tableau-demo',
    requestedPermissions: [
      {
        streamId: '*',
        level: 'read'
      }
    ],
    // set this if you don't want a popup
    returnURL: 'self#',
    // use the built-in auth button (optional)
    spanButtonID: 'pryv-button',
    callbacks: {
      initialization: function () { },
      // optional; triggered if the user isn't signed-in yet
      needSignin: function(popupUrl, pollUrl, pollRateMs) {
        $('#submitButton').hide();
        if (tableau.phase !== tableau.phaseEnum.gatherDataPhase) {
          tableau.password = null;
          tableau.username = null;
          tableau.abortForAuth();
          pyConnection = null;
        }
      },
      needValidation: null,
      signedIn: onSignedIn,
      refused: function (reason) { },
      error: function (code, message) { }
    }
  };

  pryv.Auth.setup(authSettings);


  function getPYConnection() {
    if (pyConnection) {
      if (! tableau.password) {
        tableau.password = pyConnection.auth;
        tableau.username = pyConnection.username + '.' + pyConnection.settings.domain
      }
      return pyConnection;
    }
    if (tableau.password) {
      pyConnection = new pryv.Connection(
        {url: 'https://' + tableau.username + '/',
          auth: tableau.password});
    }

    return pyConnection;
  }





//---------- data loaders




  function getLocationEvents(table, doneCallback) {

    var tableData = [];
    getEvents(function (events) {
      for (var i = 0; i < events.length; i++) {
        var event = events[i];
        if (event.type === 'position/wgs84') {
          tableData.push({
            id: event.id,
            streamId: event.streamId,
            type: event.type,
            lat: event.content.latitude,
            lon: event.content.longitude,
            time: dateFormat(event.timeLT),
            duration: event.duration
          });
        }
      }
      // Once we have all the data parsed, we send it to the Tableau table object
      table.appendRows(tableData);
      doneCallback();

    });
  }



  function getNumEvents(table, doneCallback) {

    var tableData = [];
    getEvents(function (events) {
      for (var i = 0; i < events.length; i++) {
        var event = events[i];
        if (!isNaN(parseFloat(event.content)) && isFinite(event.content)) {
          tableData.push({
            id: event.id,
            streamId: event.streamId,
            type: event.type,
            content: event.content,
            time: dateFormat(event.timeLT),
            duration: event.duration
          });
        }
      }
      // Once we have all the data parsed, we send it to the Tableau table object
      table.appendRows(tableData);
      doneCallback();

    });
  }

  var events;
  function getEvents(doneCallback) {
    if (events) return doneCallback(events);
    var filter = new pryv.Filter({limit : 10000});
    getPYConnection().events.get(filter, function (err, es) {
      var events = es;
      return doneCallback(events);
    });
  }

  function getStreams(table, doneCallback) {
    getPYConnection().streams.get(null, function(err, streams) {
      function addChilds(tableD, streamArray) {
        for (var i = 0; i < streamArray.length; i++) {
          var stream = streamArray[i];
          tableD.push(
            {
              id: stream.id,
              parentId: stream.parentId,
              name: stream.name
            }
          );
          addChilds(tableD, stream.children);
        }
      }

      var tableData = [];
      addChilds(tableData, streams);
      // Once we have all the data parsed, we send it to the Tableau table object
      table.appendRows(tableData);
      doneCallback();
    });
  }


  //---- helpers ----//


  function dateFormat(time) {
    return moment(new Date(time)).format("Y-MM-DD HH:mm:ss")
  }

})();