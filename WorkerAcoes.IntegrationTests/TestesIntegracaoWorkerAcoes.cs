using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Xunit;
using FluentAssertions;
using RabbitMQ.Client;
using MongoDB.Driver;
using Serilog;
using Serilog.Core;
using WorkerAcoes.IntegrationTests.Models;
using WorkerAcoes.IntegrationTests.Documents;

namespace WorkerAcoes.IntegrationTests
{
    public class TestesIntegracaoWorkerAcoes
    {
        private const string COD_CORRETORA = "00000";
        private const string NOME_CORRETORA = "Corretora Testes";
        private static IConfiguration Configuration { get; }
        private static Logger Logger { get; }

        static TestesIntegracaoWorkerAcoes()
        {
            Configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"appsettings.json")
                .AddEnvironmentVariables().Build();

            Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
        }

        [Theory]
        [InlineData("ABCD", 100.98)]
        [InlineData("EFGH", 200.9)]
        [InlineData("IJKL", 1_400.978)]
        public void TestarWorkerService(string codigo, double valor)
        {
            var rabbitMQConnection = Configuration["RabbitMQ:ConnectionString"];
            Logger.Information($"Conexão RabbitMQ: {rabbitMQConnection}");

            var queueName = Configuration["RabbitMQ:Queue"];
            Logger.Information($"Queue: {queueName}");

            var cotacaoAcao = new Acao()
            {
                Codigo = codigo,
                Valor = valor,
                CodCorretora = COD_CORRETORA,
                NomeCorretora = NOME_CORRETORA
            };
            var conteudoAcao = JsonSerializer.Serialize(cotacaoAcao);
            Logger.Information($"Dados: {conteudoAcao}");


            var factory = new ConnectionFactory()
            {
                Uri = new(Configuration["RabbitMQ:ConnectionString"])
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName,
                                    durable: false,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

            channel.BasicPublish(exchange: "",
                                    routingKey: queueName,
                                    basicProperties: null,
                                    body: Encoding.UTF8.GetBytes(conteudoAcao));

            Logger.Information(
                $"RabbitMQ - Envio para a fila {queueName} concluído | " +
                $"{conteudoAcao}");
            
            Logger.Information("Aguardando o processamento do Worker...");
            Thread.Sleep(
                Convert.ToInt32(Configuration["IntervaloProcessamento"]));

            var mongoDBConnection = Configuration["MongoDB:ConnectionString"];
            Logger.Information($"MongoDB Connection: {mongoDBConnection}");

            var mongoDatabase = Configuration["MongoDB:Database"];
            Logger.Information($"MongoDB Database: {mongoDatabase}");

            var mongoCollection = Configuration["MongoDB:Collection"];
            Logger.Information($"MongoDB Collection: {mongoCollection}");

            var acaoDocument = new MongoClient(mongoDBConnection)
                .GetDatabase(mongoDatabase)
                .GetCollection<AcaoDocument>(mongoCollection)
                .Find(h => h.Codigo == codigo).SingleOrDefault();

            acaoDocument.Should().NotBeNull();
            acaoDocument.Codigo.Should().Be(codigo);
            acaoDocument.Valor.Should().Be(valor);
            acaoDocument.CodCorretora.Should().Be(COD_CORRETORA);
            acaoDocument.NomeCorretora.Should().Be(NOME_CORRETORA);
            acaoDocument.HistLancamento.Should().NotBeNullOrWhiteSpace();
            acaoDocument.DataReferencia.Should().NotBeNullOrWhiteSpace();
        }
    }
}