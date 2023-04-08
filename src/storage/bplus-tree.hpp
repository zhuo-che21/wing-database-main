#ifndef BPLUS_TREE_H_
#define BPLUS_TREE_H_

#include "common/logging.hpp"

#include <cassert>
#include <filesystem>
#include <optional>
#include <stack>

#include "page-manager.hpp"

namespace wing {

/* Level 0: Leaves
 * Level 1: Inners
 * Level 2: Inners
 * ...
 * Level N: Root
 *
 * Initially the root is a leaf.
 *-----------------------------------------------------------------------------
 * Meta page:
 * Offset(B)  Length(B) Description
 * 0          1         Level num of root
 * 4          4         Root page ID
 * 8          8         Number of tuples (i.e., KV pairs)
 *-----------------------------------------------------------------------------
 * Inner page:
 * next_0 key_0 next_1 key_1 next_2 ... next_{n-1} key_{n-1} next_n
 * ^^^^^^^^^^^^ ^^^^^^^^^^^^            ^^^^^^^^^^^^^^^^^^^^ ^^^^^^
 *    Slot_0       Slot_1                    Slot_{n-1}      Special
 * Note that the lengths of keys are omitted in slots because they can be
 * deduced with the lengths of slots.
 *-----------------------------------------------------------------------------
 * Leaf page:
 * len(key_0) key_0 value_0 len(key_1) key_1 value_1 ...
 * ^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^
 *        Slot_0                   Slot_1
 *
 * len(key_{n-1}) key_{n-1} value_{n-1} prev_leaf next_leaf
 * ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^
 *            Slot_{n-1}                      Special
 * The type of len(key) is pgoff_t. Note that the lengths of values are omitted
 * in slots because they can be deduced with the lengths of slots:
 *     len(value_i) = len(Slot_i) - sizeof(pgoff_t) - len(key_i)
 */

// Parsed inner slot.
struct InnerSlot {
  // The child in this slot. See the layout of inner page above.
  pgid_t next;
  // The strict upper bound of the keys in the corresponding subtree of child.
  // i.e., all keys in the corresponding subtree of child < "strict_upper_bound"
  std::string_view strict_upper_bound;
};
// Parse the content of on-disk inner slot
InnerSlot InnerSlotParse(std::string_view slot);
// The size of the inner slot in on-disk format. In other words, the size of
// serialized inner slot using InnerSlotSerialize below.
static inline size_t InnerSlotSize(InnerSlot slot) {
  return sizeof(pgid_t) + slot.strict_upper_bound.size();
}
// Convert/Serialize the parsed inner slot to on-disk format and write it to
// the memory area starting with "addr".
void InnerSlotSerialize(char *addr, InnerSlot slot);

// Parsed leaf slot
struct LeafSlot {
  std::string_view key;
  std::string_view value;
};
// Parse the content of on-disk leaf slot
LeafSlot LeafSlotParse(std::string_view data);
// The size of the leaf slot in on-disk format. In other words, the size of
// serialized leaf slot using LeafSlotSerialize below.
static inline size_t LeafSlotSize(LeafSlot slot) {
  return sizeof(pgoff_t) + slot.key.size() + slot.value.size();
}
// Convert/Serialize the parsed leaf slot to on-disk format and write it to
// the memory area starting with "addr".
void LeafSlotSerialize(char *addr, LeafSlot slot);

template <typename Compare>
class BPlusTree {
 private:
  using Self = BPlusTree<Compare>;
  class LeafSlotKeyCompare;
  class LeafSlotCompare;
  using LeafPage = SortedPage<LeafSlotKeyCompare, LeafSlotCompare>;
 public:
  class Iter {
   public:
    Iter(const Iter&) = delete;
    Iter& operator=(const Iter&) = delete;
    Iter(std::reference_wrapper<PageManager> pgm, pgid_t page_id,
    pgid_t meta_pgid, slotid_t slot_id) :page_id_(page_id_), 
    slot_id_(slot_id_), pgm_(pgm), meta_pgid_(meta_pgid){}
    Iter(Iter&& iter) :page_id_(iter.page_id_), slot_id_(iter.slot_id_) {
      // DB_ERR("Not implemented!");
    }
    Iter& operator=(Iter&& iter) {
      page_id_ = iter.page_id_;
      slot_id_ = iter.slot_id_; 
      return *this;
      // DB_ERR("Not implemented!");
    }
    // Returns an optional key-value pair. The first std::string_view is the key
    // and the second std::string_view is the value.
    std::optional<std::pair<std::string_view, std::string_view>> Cur() {
      BPlusTree bplus = Open(pgm_, meta_pgid_);
      LeafPage leaf_p = bplus.GetLeafPage(page_id_);
      std::string_view content = leaf_p.Slot(slot_id_);
      LeafSlot s = bplus.LeafSlotParse(content);
      return std::make_optional(std::make_pair(s.key, s.value));
      // return std::make_optional(s);
      // DB_ERR("Not implemented!");
    }
    void Next() {
      BPlusTree bplus = Open(pgm_, meta_pgid_);
      LeafPage leaf = bplus.GetLeafPage(page_id_);
      if (slot_id_ >= leaf.SlotNum() )
      {
        page_id_ = bplus.GetLeafNext(leaf);
        if (bplus.GetLeafNext(leaf)==0)
        {
          return;
        }
        slot_id_ = 0;
      }
      slot_id_ = slot_id_ + 1;
      // DB_ERR("Not implemented!");
    }
    pgid_t Page_id() {
      return page_id_;
    }
    slotid_t Slot_id() {
      return slot_id_;
    }
   private:
    std::reference_wrapper<PageManager> pgm_;
    pgid_t meta_pgid_;
    pgid_t page_id_;
    slotid_t slot_id_;
  };
  BPlusTree(const Self&) = delete;
  Self& operator=(const Self&) = delete;
  BPlusTree(Self&& rhs)
    : pgm_(rhs.pgm_), meta_pgid_(rhs.meta_pgid_), comp_(rhs.comp_) {}
  Self& operator=(Self&& rhs) {
    pgm_ = std::move(rhs.pgm_);
    meta_pgid_ = rhs.meta_pgid_;
    comp_ = rhs.comp_;
    return *this;
  }
  // Free in-memory resources.
  ~BPlusTree() {
    // Do not call Destroy() here.
    // Normally you don't need to do anything here.
  }
  /* Allocate a meta page and initialize an empty B+tree.
   * The caller may get the meta page ID by BPlusTree::MetaPageID() and
   * optionally save it somewhere so that the B+tree can be reopened with it
   * in the future. You don't need to care about the persistency of the meta
   * page ID here.
   */
  static Self Create(std::reference_wrapper<PageManager> pgm) {
    Self ret(pgm, pgm.get().Allocate(), Compare());
    auto root = ret.AllocLeafPage();
    pgid_t root_id = root.ID();
    ret.UpdateRoot(root_id);
    ret.UpdateLevelNum(1);
    ret.UpdateTupleNum(0);
    // Initialize the tree here.
    // DB_ERR("Not implemented!");
    return ret;
  }
  // Open a B+tree with its meta page ID.
  static Self Open(std::reference_wrapper<PageManager> pgm, pgid_t meta_pgid) {
    return Self(pgm, meta_pgid, Compare());
  }
  // Get the meta page ID so that the caller may optionally save it somewhere
  // to reopen the B+tree with it in the future.
  inline pgid_t MetaPageID() const { return meta_pgid_; }
  // Free on-disk resources including the meta page.
  void Destroy() { pgid_t root = Root();
    std::stack<pgid_t> p_stack;
    p_stack.push(root);
    uint8_t level = LevelNum()-1;
    if (level == 0)
    {
      FreePage(std::move(GetLeafPage(root)));
      FreePage(std::move(GetMetaPage()));
    }
    else{
      InnerPage cur = GetInnerPage(root);
      pgid_t leaf_id;
      while (1)
      {
        if (cur.SlotNum() != 0)
        {
          pgid_t s = InnerSlotParse(cur.Slot(0)).next;
          if (level > 1)
          {
            level--;
            cur = GetInnerPage(s);
            p_stack.push(cur.ID());
          } else {
            leaf_id = GetLeafPage(s).ID();
            FreePage(std::move(GetLeafPage(leaf_id)));
            cur.DeleteSlot(0);
          }
        } else {
          pgid_t spa = GetInnerSpecial(cur);
          if (level > 1)
          {
            FreePage(std::move(GetInnerPage(spa)));
          } else {
            FreePage(std::move(GetLeafPage(spa)));
          }
          FreePage(std::move(cur));
          p_stack.pop();
          if (p_stack.empty())
          {
            break;
          }
          cur = GetInnerPage(p_stack.top());
          cur.DeleteSlot(0);
          level++;        
        }
      }
      FreePage(std::move(GetMetaPage()));
    }
    // DB_ERR("Not implemented!");
  }
  bool IsEmpty() {
    return (TupleNum()==0);
    // DB_ERR("Not implemented!");
  }
  /* Insert only if the key does not exists.
   * Return whether the insertion is successful.
   */
  bool Insert(std::string_view key, std::string_view value) {
    pgid_t cur = Root();
    uint8_t level = LevelNum()-1;
    LeafSlot leaf_s = {key, value};
    char* addr;
    LeafSlotSerialize(addr, leaf_s);
    std::string_view s = std::string_view(addr, LeafSlotSize(leaf_s));
    std::stack<std::pair<pgid_t,uint8_t>> par;
    if (level == 0)
    {
      slotid_t slot_id =  GetLeafPage(cur).Find(key);
      if (GetLeafPage(cur).IsInsertable(s))
      {
        return GetLeafPage(cur).InsertBeforeSlot(slot_id, s);
      }
      LeafPage l_next = AllocLeafPage();
      pgid_t l_next_id = l_next.ID();
      LeafPage cur_page = GetLeafPage(cur);
      SetLeafPrev(cur_page ,l_next_id);
      SetLeafNext(l_next, cur);
      GetLeafPage(cur).SplitInsert(l_next,s,slot_id);
      InnerPage ro = AllocInnerPage();
      pgid_t ro_id = ro.ID();
      UpdateRoot(ro_id);
      std::string_view str_upp = LeafSlotParse(GetLeafPage(cur).Slot(0)).key;
      InnerSlot root_slot = {l_next_id,str_upp};
      char* addr;
      InnerSlotSerialize(addr, root_slot);
      ro.InsertBeforeSlot(0,std::string_view(addr,InnerSlotSize(root_slot)));
      SetInnerSpecial(ro,cur);
      UpdateLevelNum(2);
      return true;
    }
    InnerPage inn = GetInnerPage(cur);
    while (level > 1)
    {
      slotid_t upper = inn.UpperBound(key);
      if (upper == inn.SlotNum())
      {
        inn = GetInnerPage(GetInnerSpecial(inn));
      }
      else {
        inn = GetInnerPage(InnerSlotParse(inn.Slot(upper)).next);
      }
      par.push(std::make_pair(inn.ID(),level));
      level--;
    }
    slotid_t upper = inn.UpperBound(key);
    LeafPage leaf = GetLeafPage(InnerSlotParse(inn.Slot(upper)).next);
    slotid_t slot_id =  leaf.Find(key);
    par.push(std::make_pair(leaf.ID(),level));
    if (leaf.IsInsertable(s))
    {
      return leaf.InsertBeforeSlot(slot_id, s);
    }
    else {
      LeafPage leaf_next = AllocLeafPage();
      pgid_t leaf_next_id = leaf_next.ID();
      leaf.SplitInsert(leaf_next,s,slot_id);
      SetLeafPrev(leaf,leaf_next_id);
      SetLeafNext(leaf_next, leaf.ID());
      
      std::string_view s_upper = LeafSlotParse(leaf.Slot(0)).key;
      InnerSlot slot_next = {leaf_next_id, s_upper};
      char* addr;
      InnerSlotSerialize(addr, slot_next);
      // std::string_view s_next = std::string_view(addr, InnerSlotSize(slot_next));
      auto po = par.top();
      par.pop();
      pgid_t pa = po.first;
      uint8_t lev = po.second;
      inn = GetInnerPage(pa);
      slotid_t upper = inn.UpperBound(key);
      if (inn.IsInsertable(std::string_view(addr, InnerSlotSize(slot_next))))
      {
        inn.InsertBeforeSlot(upper, std::string_view(addr, InnerSlotSize(slot_next))); 
      }
      while (!inn.IsInsertable(std::string_view(addr, InnerSlotSize(slot_next))))
      {
        if (lev == LevelNum()-1)
        {
          InnerPage i_next = AllocInnerPage();
          pgid_t i_next_id = i_next.ID();
          inn.SplitInsert(i_next,std::string_view(addr, InnerSlotSize(slot_next)),upper);
          InnerPage new_ro = AllocInnerPage();
          pgid_t new_ro_id = new_ro.ID();
          UpdateRoot(new_ro_id);
          std::string_view str_upp = LeafSlotParse(inn.Slot(0)).key;
          InnerSlot root_slot = {i_next_id,str_upp};
          char* addr;
          InnerSlotSerialize(addr, root_slot);
          new_ro.InsertBeforeSlot(0,std::string_view(addr,InnerSlotSize(root_slot)));
          SetInnerSpecial(new_ro,cur);
          UpdateLevelNum(LevelNum()+1);
          return true;
        }
        InnerPage next = AllocInnerPage();
        pgid_t next_id = next.ID();
        auto po = par.top();
        par.pop();
        uint8_t lev = po.second;
        inn.SplitInsert(next,std::string_view(addr, InnerSlotSize(slot_next)),upper);
        pgid_t ri = InnerSlotParse(next.Slot(next.SlotNum()-1)).next;
        SetInnerSpecial(next, ri);
        next.DeleteSlot(next.SlotNum()-1);
        // SetLeafPrev(inn,next_id);
        // SetLeafNext(next,inn.ID());
        
        std::string_view s_upper = InnerSmallestKey(inn,lev);
        InnerSlot slot_next = {next_id, s_upper};
        // char* addr;
        InnerSlotSerialize(addr, slot_next);
        // std::string_view s_next = std::string_view(addr, InnerSlotSize(slot_next));

        pgid_t pa = po.first;
        inn = GetInnerPage(pa);
        slotid_t upper = inn.UpperBound(key);
        if (inn.IsInsertable(std::string_view(addr, InnerSlotSize(slot_next))))
        {
          inn.InsertBeforeSlot(upper, std::string_view(addr, InnerSlotSize(slot_next)));
        }
      }
    }  
    return true;
    // DB_ERR("Not implemented!");
  }
  /* Update only if the key already exists.
   * Return whether the update is successful.
   */
  bool Update(std::string_view key, std::string_view value) {
    if (Delete(key))
    {
      Insert(key,value);
      return true;
    }
    return false;
    // DB_ERR("Not implemented!");
  }
  // Return the maximum key in the tree.
  // If no key exists in the tree, return std::nullopt
  std::optional<std::string> MaxKey() {
    if (IsEmpty())
    {
      return std::nullopt;
    }
    
    if (LevelNum()==1)
    {
      pgid_t root = Root();
      LeafPage r = GetLeafPage(root);
      return std::make_optional(LeafLargestKey(r));
    }
    else {
      pgid_t root = Root();
      pgid_t l = LargestLeaf(GetInnerPage(root),LevelNum()-1);
      return std::make_optional(LeafLargestKey(GetLeafPage(l)));
    }
    
    // DB_ERR("Not implemented!");
  }
  std::optional<std::string> Get(std::string_view key) {
    pgid_t cur = Root();
    uint8_t level = LevelNum()-1;
    if (level == 0)
    {
      if (GetLeafPage(cur).FindSlot(key).has_value())
    {
      return LeafSlotParse(GetLeafPage(cur).FindSlot(key)).value;
    } else {
      return std::nullopt;
    }
    }
    InnerPage inn = GetInnerPage(cur);
    while (level > 1)
    {
      slotid_t upper = inn.UpperBound(key);
      if (upper == inn.SlotNum())
      {
        inn = GetInnerPage(GetInnerSpecial(inn));
      }
      else {
        inn = GetInnerPage(InnerSlotParse(inn.Slot(upper)).next);
        }
      level--;
    }
    slotid_t upper = inn.UpperBound(key);
    LeafPage leaf = GetLeafPage(GetInnerSpecial(inn));
    if (upper == inn.SlotNum())
      {
        leaf = GetLeafPage(GetInnerSpecial(inn));
      }
    else {
      leaf = GetLeafPage(InnerSlotParse(inn.Slot(upper)).next);
      }
    if (leaf.FindSlot(key).has_value())
    {
      return LeafSlotParse(leaf.FindSlot(key)).value;
    } else {
      return std::nullopt;
    }
    // DB_ERR("Not implemented!");
  }
  // Return succeed or not.
  bool Delete(std::string_view key) {
    pgid_t cur = Root();
    uint8_t level = LevelNum()-1;
    std::stack<pgid_t> par;
    if (level == 0)
    {
      if (GetLeafPage(cur).FindSlot(key) == std::nullopt)
      {
        return false;
      }
      GetLeafPage(cur).DeleteSlotByKey(key);
      return true;
    }
    InnerPage inn = GetInnerPage(cur);
    while (level > 1)
    {
      slotid_t upper = inn.UpperBound(key);
      if (upper == inn.SlotNum())
      {
        inn = GetInnerPage(GetInnerSpecial(inn));
      }
      else {
        inn = GetInnerPage(InnerSlotParse(inn.Slot(upper)).next);
        }
      par.push(inn.ID());
      level--;
    }
    slotid_t upper = inn.UpperBound(key);
    LeafPage leaf = GetLeafPage(GetInnerSpecial(inn));
    if (upper == inn.SlotNum())
      {
        leaf = GetLeafPage(GetInnerSpecial(inn));
      }
    else {
      leaf = GetLeafPage(InnerSlotParse(inn.Slot(upper)).next);
      }
    par.push(leaf.ID());
    if (leaf.FindSlot(key) == std::nullopt)
    {
      return false;
    }
    leaf.DeleteSlotByKey(key);
    if (leaf.SlotNum()==0)
    {
      pgid_t leaf_pre = GetLeafPrev(leaf);
      pgid_t leaf_next = GetLeafNext(leaf);
      LeafPage leaf_page_pre = GetLeafPage(leaf_pre);
      LeafPage leaf_page_next = GetLeafPage(leaf_next);
      SetLeafNext(leaf_page_pre, leaf_next);
      SetLeafPrev(leaf_page_next, leaf_pre);
      FreePage(std::move(leaf));
      pgid_t pa = par.top();
      par.pop();
      inn = GetInnerPage(pa);
      slotid_t upper = inn.UpperBound(key);
      inn.DeleteSlot(upper);
      while ((inn.SlotNum()==0) && (!par.empty()))
      {
        FreePage(std::move(inn));
        pgid_t pa = par.top();
        par.pop();
        inn = GetInnerPage(pa);
        slotid_t upper = inn.UpperBound(key);
        inn.DeleteSlot(upper);
      }
    }
    if (IsEmpty())
    {
      UpdateLevelNum(1);
    }
    
    return true;
    // DB_ERR("Not implemented!");
  }
  // Logically equivalent to firstly Get(key) then Delete(key)
  std::optional<std::string> Take(std::string_view key) {
    auto v = Get(key);
    Delete(key);
    return v;
    // DB_ERR("Not implemented!");
  }
  // Return an iterator that iterates from the first element.
  Iter Begin() {
    pgid_t root = Root();
    Iter iter = {pgm_,root,meta_pgid_,0};
    return iter;
    // DB_ERR("Not implemented!");
  }
  // Return an iterator that points to the tuple with the minimum key
  // s.t. key >= "key" in argument
  Iter LowerBound(std::string_view key) {
    pgid_t cur = Root();
    uint8_t level = LevelNum()-1;
    if (level == 0)
    {
      slotid_t slot_id = GetLeafPage(cur).LowerBound(key);
      Iter iter = {cur, slot_id};
      return iter;
    }
    InnerPage inn = GetInnerPage(cur);
    while (level > 1)
    {
      slotid_t upper = inn.UpperBound(key);
      if (upper == inn.SlotNum())
      {
        inn = GetInnerPage(GetInnerSpecial(inn));
      }
      else {
        inn = GetInnerPage(InnerSlotParse(inn.Slot(upper)).next);
        }
      level--;
    }
    slotid_t upper = inn.UpperBound(key);
    LeafPage leaf = GetLeafPage(GetInnerSpecial(inn));
    if (upper == inn.SlotNum())
      {
        leaf = GetLeafPage(GetInnerSpecial(inn));
      }
    else {
      leaf = GetLeafPage(InnerSlotParse(inn.Slot(upper)).next);
      }
    slotid_t slot_id = leaf.LowerBound(key);
    Iter iter = {pgm_,cur,meta_pgid_,slot_id};
    return iter;
    // DB_ERR("Not implemented!");
  }
  // Return an iterator that points to the tuple with the minimum key
  // s.t. key > "key" in argument
  Iter UpperBound(std::string_view key) {
    pgid_t cur = Root();
    uint8_t level = LevelNum()-1;
    if (level == 0)
    {
      slotid_t slot_id = GetLeafPage(cur).UpperBound(key);
      Iter iter = {cur, slot_id};
      return iter;
    }
    InnerPage inn = GetInnerPage(cur);
    while (level > 1)
    {
      slotid_t upper = inn.UpperBound(key);
      if (upper == inn.SlotNum())
      {
        inn = GetInnerPage(GetInnerSpecial(inn));
      }
      else {
        inn = GetInnerPage(InnerSlotParse(inn.Slot(upper)).next);
        }
      level--;
    }
    slotid_t upper = inn.UpperBound(key);
    LeafPage leaf = GetLeafPage(GetInnerSpecial(inn));
    if (upper == inn.SlotNum())
      {
        leaf = GetLeafPage(GetInnerSpecial(inn));
      }
    else {
      leaf = GetLeafPage(InnerSlotParse(inn.Slot(upper)).next);
      }
    slotid_t slot_id = leaf.UpperBound(key);
    Iter iter = {pgm_, cur, meta_pgid_, slot_id};
    return iter;
    // DB_ERR("Not implemented!");
  }
  size_t TupleNum() {
    size_t sum = 0;
    Iter beg = Begin();
    slotid_t s1 = beg.Slot_id();
    pgid_t p1 = beg.Slot_id();
    beg.Next();
    slotid_t s2 = beg.Slot_id();
    pgid_t p2 = beg.Page_id();
    while ((s1 != s2) && (p1 != p2))
    {
      s1 = s2;
      p1 = p2;
      beg.Next();
      s2 = beg.Slot_id();
      p2 = beg.Page_id();
      sum++;
    }
    return sum;
    // DB_ERR("Not implemented!");
  }
 private:
  // Here we provide some helper classes/functions that you may use.

  class InnerSlotKeyCompare {
  public:
    // slot: the content of the to-be-compared inner slot.
    std::weak_ordering operator()(
      std::string_view slot, std::string_view key
    ) const {
      return comp_(InnerSlotParse(slot).strict_upper_bound, key);
    }
  private:
    InnerSlotKeyCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  class InnerSlotCompare {
  public:
    // a, b: the content of the two inner slots to be compared.
    std::weak_ordering operator()(
      std::string_view a, std::string_view b
    ) const {
      std::string_view a_key = InnerSlotParse(a).strict_upper_bound;
      std::string_view b_key = InnerSlotParse(b).strict_upper_bound;
      return comp_(a_key, b_key);
    }
  private:
    InnerSlotCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  typedef SortedPage<InnerSlotKeyCompare, InnerSlotCompare> InnerPage;

  class LeafSlotKeyCompare {
  public:
    // slot: the content of the to-be-compared leaf slot.
    std::weak_ordering operator()(
      std::string_view slot, std::string_view key
    ) const {
      return comp_(LeafSlotParse(slot).key, key);
    }
  private:
    LeafSlotKeyCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  class LeafSlotCompare {
  public:
    // a, b: the content of two to-be-compared leaf slots.
    std::weak_ordering operator()(
      std::string_view a, std::string_view b
    ) const {
      return comp_(LeafSlotParse(a).key, LeafSlotParse(b).key);
    }
  private:
    LeafSlotCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };

  BPlusTree(std::reference_wrapper<PageManager> pgm, pgid_t meta_pgid,
      const Compare& comp)
    : pgm_(pgm), meta_pgid_(meta_pgid), comp_(comp) {}

  // Reference the inner page and return a handle for it.
  inline InnerPage GetInnerPage(pgid_t pgid) {
    return pgm_.get().GetSortedPage(pgid, InnerSlotKeyCompare(comp_),
        InnerSlotCompare(comp_));
  }
  // Reference the leaf page and return a handle for it.
  inline LeafPage GetLeafPage(pgid_t pgid) {
    return pgm_.get().GetSortedPage(pgid, LeafSlotKeyCompare(comp_),
        LeafSlotCompare(comp_));
  }
  // Reference the meta page and return a handle for it.
  inline PlainPage GetMetaPage() {
    return pgm_.get().GetPlainPage(meta_pgid_);
  }

  /* PageManager::Free requires that the page is not being referenced.
   * So we have to first explicitly drop the page handle which should be the
   * only reference to the page before calling PageManager::Free to free the
   * page.
   *
   * For example, if you have a page handle "inner1" that is the only reference
   * to the underlying page, and you want to free this page on disk:
   *
   * InnerPage inner1 = GetInnerPage(pgid);
   * // This is wrong! "inner1" is still referencing this page!
   * pgm_.get().Free(inner1.ID());
   * // This is correct, because "FreePage" will drop the page handle "inner1"
   * // and thus drop the only reference to the underlying page, so that the
   * // underlying page can be safely freed with PageManager::Free.
   * FreePage(std::move(inner1));
   */
  inline void FreePage(Page&& page) {
    pgid_t id = page.ID();
    page.Drop();
    pgm_.get().Free(id);
  }

  // Allocate an inner page and return a handle that references it.
  inline InnerPage AllocInnerPage() {
    auto inner = pgm_.get().AllocSortedPage(InnerSlotKeyCompare(comp_),
        InnerSlotCompare(comp_));
    inner.Init(sizeof(pgid_t));
    return inner;
  }
  // Allocate a leaf page and return a handle that references it.
  inline LeafPage AllocLeafPage() {
    auto leaf = pgm_.get().AllocSortedPage(LeafSlotKeyCompare(comp_),
        LeafSlotCompare(comp_));
    leaf.Init(sizeof(pgid_t) * 2);
    return leaf;
  }

  // Get the right-most child
  inline pgid_t GetInnerSpecial(const InnerPage& inner) {
    return *(pgid_t *)inner.ReadSpecial(0, sizeof(pgid_t)).data();
  }
  // Set the right-most child
  inline void SetInnerSpecial(InnerPage& inner, pgid_t page) {
    inner.WriteSpecial(0, std::string_view((char *)&page, sizeof(page)));
  }
  inline pgid_t GetLeafPrev(LeafPage& leaf) {
    return *(pgid_t *)leaf.ReadSpecial(0, sizeof(pgid_t)).data();
  }
  inline void SetLeafPrev(LeafPage& leaf, pgid_t pgid) {
    leaf.WriteSpecial(0, std::string_view((char *)&pgid, sizeof(pgid)));
  }
  inline pgid_t GetLeafNext(LeafPage& leaf) {
    return *(pgid_t *)leaf.ReadSpecial(sizeof(pgid_t), sizeof(pgid_t)).data();
  }
  inline void SetLeafNext(LeafPage& leaf, pgid_t pgid) {
    std::string_view data((char *)&pgid, sizeof(pgid));
    leaf.WriteSpecial(sizeof(pgid_t), data);
  }

  inline uint8_t LevelNum() {
    return GetMetaPage().Read(0, 1)[0];
  }
  inline void UpdateLevelNum(uint8_t level_num) {
    GetMetaPage().Write(0,
      std::string_view((char *)&level_num, sizeof(level_num)));
  }
  inline pgid_t Root() {
    return *(pgid_t *)GetMetaPage().Read(4, sizeof(pgid_t)).data();
  }
  inline void UpdateRoot(pgid_t root) {
    GetMetaPage().Write(4, std::string_view((char *)&root, sizeof(root)));
  }
  inline void UpdateTupleNum(size_t num) {
    static_assert(sizeof(size_t) == 8);
    GetMetaPage().Write(8, std::string_view((char *)&num, sizeof(num)));
  }
  inline void IncreaseTupleNum(ssize_t delta) {
    size_t tuple_num = TupleNum();
    if (delta < 0)
      assert(tuple_num >= (size_t)(-delta));
    UpdateTupleNum(tuple_num + delta);
  }

  inline std::string_view LeafSmallestKey(const LeafPage& leaf) {
    assert(leaf.SlotNum() > 0);
    LeafSlot slot = LeafSlotParse(leaf.Slot(0));
    return slot.key;
  }
  inline std::string_view LeafLargestKey(const LeafPage& leaf) {
    assert(leaf.SlotNum() > 0);
    LeafSlot slot = LeafSlotParse(leaf.Slot(leaf.SlotNum() - 1));
    return slot.key;
  }

  pgid_t InnerFirstPage(const InnerPage& inner) {
    if (inner.IsEmpty())
      return GetInnerSpecial(inner);
    InnerSlot slot = InnerSlotParse(inner.Slot(0));
    return slot.next;
  }
  pgid_t InnerLastPage(const InnerPage& inner) {
     return GetInnerSpecial(inner);
  }

  pgid_t SmallestLeaf(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = InnerFirstPage(inner);
    while (--level)
      cur = InnerFirstPage(GetInnerPage(cur));
    return cur;
  }
  pgid_t LargestLeaf(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = InnerLastPage(inner);
    while (--level)
      cur = InnerLastPage(GetInnerPage(cur));
    return cur;
  }

  std::string_view InnerSmallestKey(const InnerPage& inner, uint8_t level) {
    return LeafSmallestKey(GetLeafPage(SmallestLeaf(inner, level)));
  }
  std::string_view InnerLargestKey(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = GetInnerSpecial(inner);
    level -= 1;
    while (level > 0) {
      InnerPage inner = GetInnerPage(cur);
      cur = GetInnerSpecial(inner);
      level -= 1;
    }
    return LeafLargestKey(GetLeafPage(cur));
  }

  // For Debugging
  void LeafPrint(std::ostream& out, const LeafPage& leaf,
      size_t (*key_printer)(std::ostream& out, std::string_view),
      size_t (*val_printer)(std::ostream& out, std::string_view)) {
    for (slotid_t i = 0; i < leaf.SlotNum(); ++i) {
      LeafSlot slot = LeafSlotParse(leaf.Slot(i));
      out << '(';
      key_printer(out, slot.key);
      out << ',';
      val_printer(out, slot.value);
      out << ')';
    }
  }
  void InnerPrint(std::ostream& out, InnerPage& inner, uint8_t level,
      size_t (*key_printer)(std::ostream& out, std::string_view)) {
    out << "{smallest:" << key_formatter(InnerSmallestKey(inner, level)) << ","
      "separators:[";
    for (slotid_t i = 0; i < inner.SlotNum(); ++i) {
      InnerSlot slot = InnerSlotParse(inner.Slot(i));
      key_printer(out, slot.strict_upper_bound);
      out << ',';
    }
    out << "],largest:" << key_formatter(InnerLargestKey(inner, level)) << '}';
  }
  void PrintSubtree(std::ostream& out, std::string& prefix, pgid_t pgid,
      uint8_t level, size_t (*key_printer)(std::ostream& out, std::string_view)) {
    if (level == 0) {
      LeafPage leaf = GetLeafPage(pgid);
      if (leaf.IsEmpty()) {
        out << "{Empty}";
      } else {
        out << "{smallest:";
        key_printer(out, LeafSmallestKey(leaf));
        out << ",largest:";
        key_printer(out, LeafLargestKey(leaf));
        out << "}\n";
      }
      return;
    }
    InnerPage inner = GetInnerPage(pgid);
    size_t len = 0; // Suppress maybe unitialized warning
    slotid_t slot_num = inner.SlotNum();
    assert(slot_num > 0);
    for (slotid_t i = 0; i < slot_num; ++i) {
      InnerSlot slot = InnerSlotParse(inner.Slot(i));
      if (i > 0)
        out << prefix;
      len = key_printer(out, slot.strict_upper_bound);
      out << '-';
      prefix.push_back('|');
      prefix.append(len, ' ');
      PrintSubtree(out, prefix, slot.next, level - 1, key_printer);
      prefix.resize(prefix.size() - len - 1);
    }
    out << prefix << '|';
    for (size_t i = 0; i < len; ++i)
      out << '-';
    prefix.append(len + 1, ' ');
    PrintSubtree(out, prefix, GetInnerSpecial(inner), level - 1, key_printer);
    prefix.resize(prefix.size() - len - 1);
  }
  /* Print the tree structure.
   * out: the target stream that will be printed to.
   * key_printer: print the key to the given stream, return the number of
   *  printed characters.
   */
  void Print(std::ostream& out,
      size_t (*key_printer)(std::ostream& out, std::string_view)) {
    std::string prefix;
    PrintSubtree(out, prefix, Root(), LevelNum(), key_printer);
  }
  // Some predefined key/value printers
  static size_t printer_str(std::ostream& out, std::string_view s) {
    out << s;
    return s.size();
  }
  static char to_oct(uint8_t c) {
    return c + '0';
  }
  static size_t printer_oct(std::ostream& out, std::string_view s) {
    for (uint8_t c : s)
      out << '\\' << to_oct(c >> 6) << to_oct((c >> 3) & 7) << to_oct(c & 7);
    return s.size() * 4;
  }
  static char to_hex(uint8_t c) {
    assert(0 <= c && c <= 15);
    if (0 <= c && c <= 9) {
      return c + '0';
    } else {
      return c - 10 + 'a';
    }
  }
  static size_t printer_hex(std::ostream& out, std::string_view s) {
    std::string str = fmt::format("({})", s.size());
    size_t printed = str.size();
    out << str;
    for (uint8_t c : s)
      out << to_hex(c >> 4) << to_hex(c & 15);
    return printed + s.size() * 2;
  }
  static size_t printer_mock(std::ostream&, std::string_view) { return 0; }

  std::reference_wrapper<PageManager> pgm_;
  pgid_t meta_pgid_;
  Compare comp_;
};

}

#endif	//BPLUS_TREE_H_
