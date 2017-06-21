const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const config = require('./config.js');
const app = express();

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var client = new kafka.Client(config.zookeeper);
var producer = new Producer(client);

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());



producer.on('ready', function(){
    console.log('Kafka producer is ready');
 });
	 
app.get('/health',(req, res) => {
	res.send('OK');
});

app.get('/send/message/:msg',(req, res) => {
	let msg = req.params['msg'];
	var  payloads = [
         { topic: config.topic, messages: msg, partition: config.partition }
     ];
	producer.send(payloads, function(err, data){
         if(err == null){
			res.send('Message Sent');
		 }
	 });
});



app.listen(config.PORT, function(){
	console.log('Server running on port: ' + config.PORT);
});