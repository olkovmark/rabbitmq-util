import amqp, { Connection, Channel, Options } from "amqplib";

export class RabbitMQ {
  private static connection: Connection;
  private static channel: Channel;
  private static queueList: Set<string> = new Set();
  static async connect(url: string) {
    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();
  }

  static async open() {
    if (this.channel) this.channel.close();
    this.channel = await this.connection.createChannel();
  }

  static async createExchange(
    exchange: string,
    type: "direct" | "topic" | "headers" | "fanout" | "match" | string,
    options?: Options.AssertExchange
  ) {
    await this.channel.assertExchange(exchange, type, options);
  }

  static async assertQueue(queueName) {
    if (this.queueList.has(queueName)) return;
    const res = await this.channel.assertQueue(queueName);
    this.queueList.add(queueName);
  }

  static async sendMessage(queueName, message, options?: Options.Publish) {
    if (typeof message !== "object") throw new Error("No JSON");

    await this.assertQueue(queueName);
    this.channel.sendToQueue(
      queueName,
      Buffer.from(JSON.stringify(message)),
      options
    );
  }

  static async sendToQueue(msg, data) {
    this.channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(JSON.stringify(data)),
      {
        correlationId: msg.properties.correlationId,
      }
    );
  }

  static async receiveMessage(queueName, callback) {
    await this.assertQueue(queueName);
    this.channel.consume(queueName, (msg) => {
      if (msg !== null) {
        let res;
        if (msg.properties.replyTo) {
          res = (response) => {
            this.sendToQueue(msg, response);
          };
        }
        callback(msg, res);
        this.channel.ack(msg);
      }
    });
  }

  static async sendMessageWithResponse(queueName, message): Promise<any> {
    if (typeof message !== "object") throw new Error("No JSON");

    const replyQueue = await this.channel.assertQueue("", {
      exclusive: true,
    });

    const correlationId = generateCorrelationId();

    const timer = setTimeout(() => {
      this.channel.deleteQueue(replyQueue.queue);
      throw new Error("Timeout");
    }, 5000);
    const res = new Promise((resolve, reject) => {
      this.channel.consume(
        replyQueue.queue,
        (msg) => {
          if (msg && msg.properties.correlationId === correlationId) {
            try {
              const responseData = JSON.parse(msg.content.toString());
              clearTimeout(timer);
              resolve(responseData);
            } catch (error) {
              reject(error);
            }
          }
        },
        { noAck: true }
      );
    });

    this.sendMessage(queueName, message, {
      correlationId: correlationId,
      replyTo: replyQueue.queue,
    });

    return res;
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
