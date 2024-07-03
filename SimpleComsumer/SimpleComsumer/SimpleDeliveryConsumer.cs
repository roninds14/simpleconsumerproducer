using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace SimpleComsumer
{
    public static class SimpleDeliveryConsumer
    {
        private static readonly string _rabbitMqHost = "localhost";
        private static readonly string _exchangeName = "delivery_exchange";
        private static readonly string _queueName = "Delivery";
        private static readonly string _routingKey = "";


        [FunctionName("DeliveryConsumer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            try {
                var factory = new ConnectionFactory() { 
                    HostName = _rabbitMqHost, 
                    Port = 5672,
                    UserName = "guest",
                    Password = "guest"
                };

                using var connection = factory.CreateConnection();
                using var channel = connection.CreateModel();
                channel.ExchangeDeclare(exchange: _exchangeName, type: "direct");
                channel.QueueDeclare(queue: _queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(queue: _queueName, exchange: _exchangeName, routingKey: _routingKey);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    log.LogInformation($"Received message: {message}");
                };
                channel.BasicConsume(queue: _queueName, autoAck: true, consumer: consumer);

                await Task.Delay(1000);
            }
            catch (Exception ex) {
                log.LogError($"An error occurred while connecting to RabbitMQ: {ex.Message}");
            }

            return new OkObjectResult(responseMessage);
        }
    }
}
