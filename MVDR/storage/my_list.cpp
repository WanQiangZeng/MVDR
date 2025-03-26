#include "my_list.h"
#include "helper.h"

template <typename T> void my_list<T>::push_front(const T &value) {
  Node *newNode = new Node(value);
  if (head == nullptr) {
    head = tail = newNode;
  } else {
    newNode->next = head;
    head->prev = newNode;
    head = newNode;
  }
  ++list_size; // Increment size
}

template <typename T> void my_list<T>::push_back(const T &value) {
  Node *newNode = new Node(value);
  if (tail == nullptr) {
    head = tail = newNode;
  } else {
    newNode->prev = tail;
    tail->next = newNode;
    tail = newNode;
  }
  ++list_size; // Increment size
}

template <typename T> void my_list<T>::pop_front() {
  if (head == nullptr)
    return;
  Node *temp = head;
  head = head->next;
  if (head != nullptr) {
    head->prev = nullptr;
  } else {
    tail = nullptr;
  }
  delete temp;
  --list_size; // Decrement size
}

template <typename T> void my_list<T>::pop_back() {
  if (tail == nullptr)
    return;
  Node *temp = tail;
  tail = tail->prev;
  if (tail != nullptr) {
    tail->next = nullptr;
  } else {
    head = nullptr;
  }
  delete temp;
  --list_size; // Decrement size
}

template <typename T> void my_list<T>::clear() {
  while (head != nullptr) {
    pop_front();
  }
}

template <typename T> bool my_list<T>::empty() const { return list_size == 0; }

template <typename T> size_t my_list<T>::size() const { return list_size; }

template <typename T> T &my_list<T>::front() { return head->data; }

template <typename T> T &my_list<T>::back() { return tail->data; }

template <typename T> typename my_list<T>::Node *my_list<T>::erase(Node *node) {
  if (node == nullptr)
    return nullptr;

  Node *nextNode = node->next;

  if (node->prev != nullptr) {
    node->prev->next = node->next;
  } else {
    head = node->next;
  }

  if (node->next != nullptr) {
    node->next->prev = node->prev;
  } else {
    tail = node->prev;
  }

  delete node;
  --list_size; // Decrement size

  return nextNode;
}

template <typename T>
void my_list<T>::insert_after(Node *node, const T &value) {
  if (node == nullptr)
    return;

  Node *newNode = new Node(value);
  newNode->prev = node;
  newNode->next = node->next;

  if (node->next != nullptr) {
    node->next->prev = newNode;
  } else {
    tail = newNode;
  }

  node->next = newNode;
  ++list_size; // Increment size
}

template <typename T>
void my_list<T>::insert_before(Node *node, const T &value) {
  if (node == nullptr)
    return;

  Node *newNode = new Node(value);
  newNode->next = node;
  newNode->prev = node->prev;

  if (node->prev != nullptr) {
    node->prev->next = newNode;
  } else {
    head = newNode;
  }

  node->prev = newNode;
  ++list_size; // Increment size
}

// 实例化模板
template class my_list<mv_data>;
template class my_list<id_data>;
template class my_list<uint64_t>;
template class my_list<int64_t>;
template class my_list<pair<uint64_t, access_t>>;
