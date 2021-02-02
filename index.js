const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const mongoose = require('mongoose');
const avro = require('avsc');
var fs = require('fs');

require('dotenv/config');

// Use bodyparser to collect json object
app.use(bodyParser.json());

// connect to db
mongoose.connect(
  process.env.DB_URL,
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  },
  () => {
    console.log('DB connected');
  }
);

// AVSC Schema
const type = avro.Type.forSchema({
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
});

// routes
app.get('/', (req, res, next) => {
  try {
    mongoose.connection.db.collection('sensor_logs', async (err, collection) => {
      let data = await collection
        .find(
          {
            gateway_id: '0xC4F3125D5A54',
          },
          { limit: 10 }
        )
        .toArray();
      console.log('🚀 ~ file: index.js ~ line 30 ~ mongoose.connection.db.collection ~ data', data);

      const buf = type.toBuffer(data); // Encoded buffer.
      const val = type.fromBuffer(buf);

      console.log('deserialized object: ', JSON.stringify(val, null, 4)); // pretty print deserialized result

      const full_filename = './tmp/avro_buf.avro';
      fs.writeFile(full_filename, buf, (err) => {
        if (err) {
          return console.log(err);
        }

        console.log("The file was saved to '" + full_filename + "'");
      });

      res.send('Success');
    });
  } catch (err) {
    res.send(err);
  }
});

app.get('/download', (req, res, next) => {
  fs.readFile('tmp/2021-01-27T00%3A00%3A35.133Z.avro', (err, data) => {
    if (err) {
      return console.log('🚀 ~ file: index.js ~ line 83 ~ fs.readFile ~ err', err);
    }
    console.log('🚀 ~ file: index.js ~ line 91 ~ fs.readFile ~ data', data);
    const val = type.fromBuffer(data);
    console.log('🚀 ~ file: index.js ~ line 101 ~ fs.readFile ~ val', val);
    const full_filename = './tmp/2021-01-27.json';
    fs.writeFile(full_filename, val, (err) => {
      if (err) {
        return console.log(err);
      }
      console.log("The file was saved to '" + full_filename + "'");
    });
    res.send('Success');
  });
});

// Listening to the server
app.listen(process.env.PORT, () => {
  console.log('app running on port', process.env.PORT);
});
