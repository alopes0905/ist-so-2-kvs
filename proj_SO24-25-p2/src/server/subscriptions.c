#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include "subscriptions.h"
#include "constants.h"
#include "src/common/constants.h"

typedef struct Subscription {
    char key[MAX_STRING_SIZE];
    int notif_fd;
    struct Subscription *next;
} Subscription;

Subscription *subscriptions = NULL;
pthread_mutex_t subscriptions_mutex = PTHREAD_MUTEX_INITIALIZER;

void handle_signal() {
    pthread_mutex_lock(&subscriptions_mutex);
    Subscription *current = subscriptions;
    while (current) {
        Subscription *next = current->next;
        free(current);
        current = next;
    }
    subscriptions = NULL;
    pthread_mutex_unlock(&subscriptions_mutex);
}

void add_subscription(const char *key, int notif_fd) {
    pthread_mutex_lock(&subscriptions_mutex);
    Subscription *new_sub = malloc(sizeof(Subscription));
    if (new_sub == NULL) {
        perror("Failed to allocate memory for new subscription");
        pthread_mutex_unlock(&subscriptions_mutex);
        return;
    }
    strncpy(new_sub->key, key, MAX_STRING_SIZE);
    new_sub->notif_fd = notif_fd;
    new_sub->next = subscriptions;
    subscriptions = new_sub;
    pthread_mutex_unlock(&subscriptions_mutex);
}

int remove_subscription(const char *key, int notif_fd) {
    pthread_mutex_lock(&subscriptions_mutex);
    Subscription **current = &subscriptions;
    int subscription_exists = 0;
    while (*current) {
        Subscription *entry = *current;
        if (strncmp(entry->key, key, MAX_STRING_SIZE) == 0 && entry->notif_fd == notif_fd) {
            *current = entry->next;
            free(entry);
            subscription_exists = 1;
            break;
        }
        current = &entry->next;
    }
    pthread_mutex_unlock(&subscriptions_mutex);
    return subscription_exists;
}

void notify_subscribers(const char *key, const char *value) {
    pthread_mutex_lock(&subscriptions_mutex);
    Subscription *current = subscriptions;
    while (current) {
        if (strncmp(current->key, key, MAX_STRING_SIZE) == 0) {
            if (current->notif_fd != -1) {
                char message[2 * MAX_STRING_SIZE + 2];
                snprintf(message, sizeof(message), "%s|%s", key, value);
                write(current->notif_fd, message, strlen(message) + 1);
            }
        }
        current = current->next;
    }
    pthread_mutex_unlock(&subscriptions_mutex);
}