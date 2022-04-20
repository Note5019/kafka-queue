<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', env('BOOTSTRAP_SERVERS'));
        $conf->set('security.protocol', env('SECURITY_PROTOCOL'));
        $conf->set('sasl.mechanism', env('SASL_MECHANISMS'));
        $conf->set('sasl.username', env('SASL_USERNAME'));
        $conf->set('sasl.password', env('SASL_PASSWORD'));

        $producer = new \RdKafka\Producer($conf);
        
        $conf->set('group.id', env('GROUP_ID'));
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
