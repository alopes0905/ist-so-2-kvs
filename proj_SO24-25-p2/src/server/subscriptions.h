#ifndef SUBSCRIPTIONS_H
#define SUBSCRIPTIONS_H
#include "src/common/constants.h"

void handle_signal();

void add_subscription(const char *key, int notif_fd);

int remove_subscription(const char *key, int notif_fd);

void notify_subscribers(const char *key, const char *value);

void delete_subscriptions();

#endif // SUBSCRIPTIONS_H