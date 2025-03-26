#pragma once

#ifndef MY_LIST_H
#define MY_LIST_H

#include <cstddef>

template <typename T> class my_list {
public:
  struct Node {
    T data;
    Node *next = NULL;
    Node *prev = NULL;
    Node(const T &data) : data(data), next(nullptr), prev(nullptr) {}
  };

  Node *head = NULL;
  Node *tail = NULL;
  size_t list_size; // Add a member variable to store the size

  my_list() : head(nullptr), tail(nullptr), list_size(0) {}

  ~my_list() { clear(); }

  void push_front(const T &value);
  void push_back(const T &value);
  void pop_front();
  void pop_back();
  void clear();
  bool empty() const;
  size_t size() const;     // Add size method
  Node *erase(Node *node); // Add erase method
  T &front();
  T &back();
  // 在指定节点后插入
  void insert_after(Node *node, const T &value);
  // 在指定节点前插入
  void insert_before(Node *node, const T &value);
};

#endif // MY_LIST_H