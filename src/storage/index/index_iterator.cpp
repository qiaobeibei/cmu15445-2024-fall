/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), page_id_(page_id), index_(index), result_({KeyType{}, ValueType{}}) {
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto page_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = page_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  result_.first = leaf_page->KeyAt(index_);
  result_.second = leaf_page->ValueAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return index_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  // std::cout << "page_id: " << page_guard_.GetPageId() << " index: " << index_ << std::endl;
  return result_;
}

// ����bootcamp�е����ӣ�return��Ӧ�û��ǵ������Լ�������������Ҫֱ���޸ĵ������Լ��ĳ�Ա�����
// return *this
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  ReadPageGuard page_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = page_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  // ��index�����󣬳����˵�ǰҳ���size��С����ͨ��next_page_id��������Ϣ
  if (index_ >= leaf_page->GetSize()) {
    page_id_t next_page_id = leaf_page->GetNextPageId();
    // ����Ѿ��������һ���������һ����ֵ�ԣ��򽫵���������ΪEnd()
    if (next_page_id == INVALID_PAGE_ID) {
      index_ = -1;
      page_id_ = next_page_id;
      return *this;
    }
    index_ = 0;
    page_id_ = next_page_id;

    page_guard = bpm_->ReadPage(page_id_);
    leaf_page = page_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }

  result_ = {leaf_page->KeyAt(index_), leaf_page->ValueAt(index_)};
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub