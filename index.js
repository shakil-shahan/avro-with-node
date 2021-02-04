const avro = require('avsc');
var fs = require('fs');
var path = require('path');

// ##################################################################
// =================== Input File Location/Path Here ================

const filePath = './../../Downloads/2021-02-03T00%3A00%3A33.600Z.avro';

// Example
// const filePath = '/home/root/collection/file.avro';

// =================== Input File Location/Path Here ================
// ##################################################################

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

// function to decode .avro file
const decodeAvro = () => {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      console.warn('Error =>', err);
    } else {
      try {
        const type = avro.Type.forSchema(sampleSenscomAzureSchema);
        const val = type.fromBuffer(data);
        const dir = './decoded';
        if (!fs.existsSync(dir)) {
          console.log('hello');
          fs.mkdirSync(dir);
        }
        const decodedFileName = 'decoded/' + path.basename(filePath).split('.').slice(0, -1).join('.') + '.json';
        fs.writeFile(decodedFileName, val, (err) => {
          if (err) {
            console.error('Error =>', err);
          }
          const savedFileLocation = __dirname + '/' + decodedFileName;
          console.log({ message: 'File decoded successfully.', DecodedFilePath: savedFileLocation });
        });
      } catch (err) {
        console.error('Error =>', err);
      }
    }
  });
};

// call the function
decodeAvro();
