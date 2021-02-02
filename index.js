const express = require('express');
const bodyParser = require('body-parser');
const avro = require('avsc');
var multer = require('multer');
var fs = require('fs');
var path = require('path');

const app = express();
var upload = multer();

require('dotenv/config');

// middleware
// Use bodyparser to collect json object
app.use(bodyParser.json());

// AVSC Schema
const sampleSenscomAzureSchema = {
  type: 'array',
  items: {
    type: 'record',
    fields: [
      { name: '_id', type: 'string' },
      { name: 'type', type: 'string' },
      { name: 'timestamp', type: 'double' },
      { name: 'gateway_id', type: 'string' },
      { name: 'sensor_id', type: 'string' },
      {
        name: 'jsondata',
        type: {
          type: 'record',
          fields: [
            { name: 'Humid1', type: 'double' },
            { name: 'Humid2', type: 'double' },
            { name: 'Humid3', type: 'double' },
            { name: 'Humid4', type: 'double' },
            { name: 'Temp1', type: 'double' },
            { name: 'Temp2', type: 'double' },
            { name: 'Temp3', type: 'double' },
            { name: 'Temp4', type: 'double' },
            { name: 'Status', type: 'double' },
          ],
        },
      },
      { name: 'createdAt', type: { type: 'string', logicalType: 'date' } },
    ],
  },
};

// routes
app.post('/', upload.single('avro'), (req, res) => {
  if (!req.file) {
    return res.send('You must have select a file.');
  } else {
    if (req.file.mimetype !== 'application/octet-stream' && path.extname(req.file.originalname) !== '.avro') {
      return res.send('You must have select an .avro file.');
    } else {
      try {
        const type = avro.Type.forSchema(sampleSenscomAzureSchema);
        const val = type.fromBuffer(req.file.buffer);
        const decodedFileName = 'decoded/' + req.file.originalname.split('.').slice(0, -1).join('.') + '.json';
        fs.writeFile(decodedFileName, val, (err) => {
          if (err) {
            return res.send({ message: 'Error to save data in a file.', data: val });
          }
          const savedFileLocation = __dirname + '/' + decodedFileName;
          return res.send({ message: `Decoded file path: ${savedFileLocation}.`, data: val });
        });
      } catch {
        return res.send('Schema mismatched. Try with senscom azure .avro file');
      }
    }
  }
});

// Listening to the server
app.listen(process.env.PORT, () => {
  console.log('app running on port', process.env.PORT);
});
