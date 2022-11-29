const { Kafka } = require("kafkajs");

const kafka = new Kafka({
	clientId: "testapp",
	brokers: ["localhost:9092"]
});

const producer = kafka.producer();
const consumer = kafka.consumer({ 
	groupId: 'test-group' 
});

(async function() { 
	
	await producer.connect();

	await producer.send({
		topic: "learnkfk",
		messages: [
			{ value: "another message" },
		],
	});

	await producer.disconnect();

	await consumer.connect();
	await consumer.subscribe({ 
		topic: 'learnkfk', 
		fromBeginning: true 
	});

	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			console.log({
				value: message.value.toString(),
			})
		},
	});
})();
