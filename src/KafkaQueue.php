<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer;
    protected $producer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
    }
    
    public function push($job, $data = '', $queue = null)
    {
        echo "push job!\n";
        \Log::info('test');
        \Log::info('$this->producer: ' . print_r($this->producer, true));
        \Log::info('$this->producer: ' . print_r(gettype($this->producer), true));
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        
        \Log::info('$topic: ' . print_r($topic, true));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, 'hellllo');
        // $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(100);
        echo "pushed job!\n";
    }
    
    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
    }

    public function pop($queue = null)
    {
        // echo "hello \n" ;
        $this->consumer->subscribe([$queue ?? env('KAFKA_QUEUE')]);

        $message = $this->consumer->consume(120*1000);

        switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump($message->payload);
                    break;
                case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more message; will wait for more\n";
                    break;
                case \RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "No more message; will wait for more\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
    }
}
