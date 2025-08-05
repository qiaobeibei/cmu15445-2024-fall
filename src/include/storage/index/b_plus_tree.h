/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // 记录tree搜索过程中各个结点中的经过的键的index
  std::deque<int> indexes_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  // 注意类模板中的ValueType是叶子结点的值类型，内部节点的值类型为page_id_t
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  /*****************************************************************************
   * INDEX ITERATOR
   *****************************************************************************/
  /*
   * Input parameter is void, find the leftmost leaf page first, then construct
   * index iterator
   * @return : index iterator
   */
  auto Begin() -> INDEXITERATOR_TYPE {
    Context ctx;

    ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
    auto head_page = guard.As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = head_page->root_page_id_;
    guard.Drop();

    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }

    // 找到最左侧的叶子结点
    ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
    auto page = page_guard.As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal_page = static_cast<const InternalPage *>(page);
      page_id_t page_id = internal_page->ValueAt(0);
      page_guard = bpm_->ReadPage(page_id);
      page = page_guard.As<BPlusTreePage>();
    }
    return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), 0);
  }

  /*
   * Input parameter is void, construct an index iterator representing the end
   * of the key/value pair in the leaf node
   * @return : index iterator
   */
  auto End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

  /*
   * Input parameter is low key, find the leaf page that contains the input key
   * first, then construct index iterator
   * @return : index iterator
   */
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    Context ctx;

    ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
    auto head_page = guard.As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = head_page->root_page_id_;
    guard.Drop();

    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }

    ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
    auto page = page_guard.As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      int index = KeyBinarySearch(page, key);
      auto internal_page = static_cast<const InternalPage *>(page);
      page_id_t page_id = internal_page->ValueAt(index);
      page_guard = bpm_->ReadPage(page_id);
      page = page_guard.As<BPlusTreePage>();
    }
    // 当前结点为叶子结点时
    int index = KeyBinarySearch(page, key);
    if (index == -1) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }
    return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), index);
  }

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  /**
   * 使用二分查找进行键查找的函数
   */
  auto KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) -> int;

  /**
   * insert时查找叶子结点中插入位置的函数
   */
  auto IndexBinarySearchLeaf(LeafPage *page, const KeyType &key) -> int;

  /**
   * 向左sibling结点借用键值的函数
   */
  void BorrowFromLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page, int index);

  /**
   * 向右sibling结点借用键值的函数
   */
  void BorrowFromRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page, int index);

  /**
   * 与左sibling结点合并的函数
   */
  void MergeWithLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page, int index);

  /**
   * 与右sibling结点合并的函数
   */
  void MergeWithRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page, int index);

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log_;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub