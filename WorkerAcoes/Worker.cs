using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WorkerAcoes.Data;
using WorkerAcoes.Models;
using WorkerAcoes.Validators;

namespace WorkerAcoes
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly AcoesRepository _repository;
        private readonly string _queue;
        private readonly int _intervaloMensagemWorkerAtivo;

        public Worker(ILogger<Worker> logger, IConfiguration configuration,
            AcoesRepository repository)
        {
            _logger = logger;
            _configuration = configuration;
            _repository = repository;

            _queue = configuration["RabbitMQ:Queue"];
            _intervaloMensagemWorkerAtivo =
                Convert.ToInt32(configuration["IntervaloMensagemWorkerAtivo"]);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Queue = {_queue}");
            _logger.LogInformation("Aguardando mensagens...");

            var factory = new ConnectionFactory()
            {
                Uri = new Uri(_configuration["RabbitMQ:ConnectionString"])
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: _queue,
                                durable: false,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += ProcessarAcao;
            channel.BasicConsume(queue: _queue,
                autoAck: true,
                consumer: consumer);

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    $"Worker ativo em: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                await Task.Delay(_intervaloMensagemWorkerAtivo, stoppingToken);
            }
        }

        private void ProcessarAcao(
            object sender, BasicDeliverEventArgs e)
        {
            var dados = Encoding.UTF8.GetString(e.Body.ToArray());
            _logger.LogInformation(
                $"[{_queue} | Nova mensagem] " + dados);

            Acao acao;            
            try
            {
                acao = JsonSerializer.Deserialize<Acao>(dados,
                    new JsonSerializerOptions()
                    {
                        PropertyNameCaseInsensitive = true
                    });
            }
            catch
            {
                acao = null;
            }

            if (acao is not null &&
                new AcaoValidator().Validate(acao).IsValid)
            {
                try
                {
                    _repository.Save(acao);
                    _logger.LogInformation("Ação registrada com sucesso!");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Erro durante a gravação: {ex.Message}");
                }
            }
            else
            {
                _logger.LogError("Dados inválidos para a Ação");
            } 
        }
    }
}