
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

const int EVENT_COUNT = 5;

typedef struct {
    int number;
    int value;
} Event;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    int ready;
    int stopping;
    void* data;
} Monitor;

Monitor monitor;

void* producer(void* arg)
{
    (void)arg;

    for (int i = 1; i <= EVENT_COUNT; )
    {
        sleep(1);

        pthread_mutex_lock(&monitor.mutex);

        if (monitor.ready == 1)
        {
            pthread_mutex_unlock(&monitor.mutex);
            continue;
        }

        Event* event = malloc(sizeof(Event));

        if (event == NULL)
        {
            monitor.stopping = 1;
            pthread_cond_broadcast(&monitor.condition);
            pthread_mutex_unlock(&monitor.mutex);
            return NULL;
        }

        event->number = i;
        event->value = i * 10;

        monitor.data = event;
        monitor.ready = 1;

        printf("Producer^ send thread #%d\n", i);
        fflush(stdout);

        pthread_cond_signal(&monitor.condition);
        pthread_mutex_unlock(&monitor.mutex);

        i++;
    }

    pthread_mutex_lock(&monitor.mutex);

    monitor.stopping = 1;
    pthread_cond_broadcast(&monitor.condition);

    pthread_mutex_unlock(&monitor.mutex);

    return NULL;
}

void* consumer(void* arg)
{
    (void)arg;

    while (1)
    {
        pthread_mutex_lock(&monitor.mutex);

        while (monitor.ready == 0 && monitor.stopping == 0)
        {
            pthread_cond_wait(&monitor.condition, &monitor.mutex);
        }

        if (monitor.ready == 0 && monitor.stopping == 1)
        {
            pthread_mutex_unlock(&monitor.mutex);
            break;
        }

        Event* event = (Event*)monitor.data;

        if (event == NULL)
        {
            monitor.ready = 0;
            pthread_mutex_unlock(&monitor.mutex);
            continue;
        }

        monitor.data = NULL;
        monitor.ready = 0;

        event->value *= 2;

        printf("Consumer: receive thread #%d, value = %d\n",
            event->number, event->value);
        fflush(stdout);

        free(event);

        pthread_mutex_unlock(&monitor.mutex);
    }

    return NULL;
}

int main(void)
{
    pthread_t producer_thread;
    pthread_t consumer_thread;

    pthread_mutex_init(&monitor.mutex, NULL);
    pthread_cond_init(&monitor.condition, NULL);

    monitor.ready = 0;
    monitor.stopping = 0;
    monitor.data = NULL;

    pthread_create(&consumer_thread, NULL, consumer, NULL);
    pthread_create(&producer_thread, NULL, producer, NULL);

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);

    pthread_cond_destroy(&monitor.condition);
    pthread_mutex_destroy(&monitor.mutex);

    return 0;
}
