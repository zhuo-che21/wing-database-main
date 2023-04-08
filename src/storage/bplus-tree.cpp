#include "bplus-tree.hpp"

namespace wing {

InnerSlot InnerSlotParse(std::string_view slot) {
	std::string s1{slot.substr(0,sizeof(pgid_t))};
	pgid_t next = std::stoi(s1.c_str());
	std::string_view key = slot.substr(sizeof(pgid_t));
	InnerSlot s = {next,key};
	return s;
	// DB_ERR("Not implemented yet!");
}
void InnerSlotSerialize(char *s, InnerSlot slot) {
	pgoff_t next_len = sizeof(slot.next);
	pgoff_t strict_len = sizeof(slot.strict_upper_bound);
	memcpy(s, &slot.next, next_len);
	memcpy(s+next_len, slot.strict_upper_bound.data(), strict_len);
	// DB_ERR("Not implemented yet!");
}

LeafSlot LeafSlotParse(std::string_view slot) {
	pgoff_t key_leng;
	memcpy(&key_leng, slot.data(), sizeof(key_leng));
	std::string_view key = slot.substr(sizeof(key_leng),key_leng);
	std::string_view value = slot.substr(key_leng+sizeof(key_leng));
	LeafSlot s = {key, value};
	return s;
	// DB_ERR("Not implemented yet!");
}
void LeafSlotSerialize(char *s, LeafSlot slot) {
	pgoff_t key_len = sizeof(slot.key);
	pgoff_t value_len = sizeof(slot.value);
	memcpy(s, &key_len, sizeof(pgoff_t));
	memcpy(s+sizeof(pgoff_t), slot.key.data(), key_len);
	memcpy(s+key_len+sizeof(pgoff_t), slot.value.data(), value_len);
	// DB_ERR("Not implemented yet!");
}

}
