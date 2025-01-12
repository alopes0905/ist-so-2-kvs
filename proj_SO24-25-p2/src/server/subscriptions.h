#ifndef SUBSCRIPTIONS_H
#define SUBSCRIPTIONS_H

void add_subscription(const char *key, const char *notif_pipe_path);

int remove_subscription(const char *key, const char *notif_pipe_path);

void notify_subscribers(const char *key, const char *value);

#endif // SUBSCRIPTIONS_H