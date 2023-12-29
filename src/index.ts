import amqp from "amqplib";

export class RabbitMQ {
  private static connection;
  private static channel;
  static async connect(url: string) {
    try {
      this.connection = await amqp.connect(url);
      this.channel = await this.connection.createChannel();
    } catch (error) {
      console.error("Error connecting to RabbitMQ:", error.message);
    }
  }

  static assertQueue(queueName) {
    this.channel.assertQueue(queueName);
  }

  static async sendMessage(queueName, message) {
    try {
      await this.channel.assertQueue(queueName);
      await this.channel.sendToQueue(
        queueName,
        Buffer.from(JSON.stringify(message))
      );
      console.log(`Sent message to queue "${queueName}":`, message);
    } catch (error) {
      console.error("Error sending message:", error.message);
    }
  }

  static async sendToQueue(msg, data) {
    try {
      this.channel.sendToQueue(
        msg.properties.replyTo,
        Buffer.from(JSON.stringify(data)),
        {
          correlationId: msg.properties.correlationId,
        },
        { noAck: true }
      );
    } catch (error) {
      console.error("Error sending message:", error.message);
    }
  }

  static async receiveMessage(queueName, callback) {
    try {
      await this.channel.assertQueue(queueName);
      console.log(`Waiting for messages in queue "${queueName}"`);

      this.channel.consume(queueName, (msg) => {
        if (msg !== null) {
          console.log(msg.content.toString());
          try {
            const message = JSON.parse(msg.content.toString());
            console.log(`Received message from queue "${queueName}":`, message);
          } catch (error) {}
          callback(msg);
          this.channel.ack(msg);
        }
      });
    } catch (error) {
      console.error("Error receiving message:", error.message);
    }
  }

  static async sendMessageWithResponse(queueName, message, callback) {
    try {
      const replyQueue = await this.channel.assertQueue("", {
        exclusive: true,
      }); // Створюємо унікальну чергу для відповідей

      const correlationId = generateCorrelationId(); // Генеруємо унікальний кореляційний ідентифікатор

      // Споживач (consumer) отримує відповідь
      this.channel.consume(
        replyQueue.queue,
        (msg) => {
          if (msg.properties.correlationId === correlationId) {
            const responseData = JSON.parse(msg.content.toString());
            console.log("Received response from Service 2:", responseData);

            // Тут ви можете обробити відповідь та відправити її клієнту API

            // Закриваємо з'єднання
            this.channel.close();
            this.connection.close();
          }
        },
        { noAck: true }
      );

      // Відправляємо дані до Сервісу 2 з кореляційним ідентифікатором та вказуємо чергу для відповідей
      this.channel.sendToQueue(queueName, Buffer.from(message), {
        correlationId: correlationId,
        replyTo: replyQueue.queue,
      });
    } catch (error) {
      console.error("Error sending message:", error.message);
    }
  }

  static async close() {
    if (this.connection) {
      await this.channel.close();
      await this.connection.close();
      console.log("Connection to RabbitMQ closed");
    }
  }
}

function generateCorrelationId() {
  return Math.random().toString() + Date.now().toString();
}
