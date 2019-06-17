#pragma once
#include <vector>
#include <stdio.h>
#include <string.h>
#include "BufferManager.h"
#include "Minisql.h"
#include <string>
using namespace std;

static BufferManager bm;
using offsetNumber = int;
using position = size_t;

template <typename KeyType>
class TreeNode
{
public:
    bool isLeaf = false;
    size_t keysCount = 0;
    TreeNode* nextLeaf = nullptr;
    TreeNode* parent = nullptr;

    vector<KeyType> Keys;
    vector<offsetNumber> Values;
    vector<TreeNode*> Childs;

private:
    int degree;

public:
    TreeNode(int degree, bool isLeaf = false);
    ~TreeNode();

public:
    bool isRoot(); // TODO:better fun or var
    bool search(KeyType key, position& index);

public:
    bool removeAt(position index);
    position addKey(const KeyType& key);                   //addKey to nonleaf
    position addKey(const KeyType& key, offsetNumber val); //addKey to leaf
    TreeNode* splite(KeyType& key);
};

template <typename KeyType>
class BPlusTree
{
    using NodePtr = TreeNode<KeyType>*;

private:
    struct searchNodeParse
    {
        NodePtr pNode;
        position index;
        bool isFound;
    };

private:
    fileNode* file; // the filenode of this tree
    int degree;
    int keySize; // the size of key
    NodePtr leafHead; // the head of the leaf node
    NodePtr root;
    size_t keysCount;
    size_t level;
    size_t nodeCount;
    string fileName;

public:
    BPlusTree(string m_naem, int keySize, int degree);
    ~BPlusTree();
    bool deleteKey(KeyType& key);
    bool insertKey(const KeyType& key, const offsetNumber val);
    offsetNumber search(const KeyType& key);
    void dropTree(NodePtr node);
    void readFromDisk();
    void readFromDisk(blockNode* btmp);
    void writeBackToDisk();

private:
    bool adjustAfterDelete(NodePtr pNode);
    bool adjustAfterInsert(NodePtr pNode);
    void findToLeaf(NodePtr pNode, KeyType key, searchNodeParse& snp);
    void init_tree(); //TODO:why not in instructor
};

//-----------------------------------------------------
template <typename KeyType>
TreeNode<KeyType>::TreeNode(int degree, bool isLeaf)
{
    this->degree = degree;
    this->isLeaf = isLeaf;
    this->keysCount = 0;
    this->parent = nullptr;
    this->nextLeaf = nullptr;
    //XXX: 是否有问题？
    this->Keys = vector<KeyType>(degree, KeyType());
    this->Values = vector<offsetNumber>(degree, offsetNumber());
    this->Childs = vector<TreeNode*>(degree + 1, nullptr);
}
template <typename KeyType>
TreeNode<KeyType>::~TreeNode()
{
}

template <typename KeyType>
bool TreeNode<KeyType>::isRoot()
{
    return parent == nullptr;
}

template <typename KeyType>
bool TreeNode<KeyType>::search(KeyType key, position& index)
{
    //case 1: no key
    if (keysCount == 0)
    {
        index = 0;
        return false;
    }
    //case 2: out of range
    if (key > Keys[keysCount - 1])
    {
        index = keysCount;
        return false;
    }
    else if (key < Keys[0])
    {
        index = 0;
        return false;
    }
    //case 3: numberOfKeys<20 => linear search
    if (keysCount < 20)
    {
        for (size_t i = 0; i < keysCount; i++)
        {
            if (key == Keys[i])
            {
                index = i;
                return true; //only in this contion
            }
            else if (key > Keys[i])
            {
                continue;
            }
            else if (key < Keys[i])
            {
                index = i;
                return false;
            }
        }
    }
    //case 4: numberOfKeys>=20 => binary search
    if (keysCount >= 20)
    {
        int left = 0, right = keysCount;
        int mid;
        while (left < right)
        {
            mid = left + (right - left) / 2;
            if (key > Keys[mid])
            {
                left = mid + 1;
            }
            else
            {
                right = mid;
            }
        }
        //NOTE: in this moment,always have left==right;
        index = left;
        if (key == Keys[left])
        {
            index = left;
            return true;
        }
        else if (key < Keys[left])
        {
            index = left;
            return false;
        }
        else if (key > Keys[left])
        {
            index = left + 1;
            return false;
        }
    }
}

template <typename KeyType>
bool TreeNode<KeyType>::removeAt(position index)
{
    if (index > keysCount)
    {
        cout << "Error:In removeAt(position index), index is more than numberOfKeys!" << endl;
        return false;
    }
    //remove key from Keys
    for (size_t i = index; i < keysCount - 1; i++)
    {
        Keys[i] = Keys[i + 1];
    }
    Keys[keysCount - 1] = KeyType();

    if (isLeaf)
    {
        for (size_t i = index; i < keysCount - 1; i++)
        {
            Values[i] = Values[i + 1];
        }
        Values[keysCount - 1] = offsetNumber();
    }
    else
    {

        for (size_t i = index + 1; i < keysCount; i++)
        {
            Childs[i] = Childs[i + 1];
        }

        Childs[keysCount] = nullptr;
    }
    keysCount--;
    return true;
}

template <typename KeyType>
position TreeNode<KeyType>::addKey(const KeyType& key)
{
    if (keysCount == 0)
    {
        Keys[0] = key;
        keysCount++;
        return 0;
    }
    position index = 0;
    bool exist = search(key, index); //NOTE: index changed by reference
    if (exist)
    {
        cout << "Error:In addKey(Keytype &key),key has already in the tree!" << endl;
        exit(3); //TODO:?????why 3
    }
    else // addKey the key into the node
    {
        for (size_t i = keysCount; i > index; i--)
            Keys[i] = Keys[i - 1];

        Keys[index] = key;

        for (size_t i = keysCount + 1; i > index + 1; i--)
            Childs[i] = Childs[i - 1];
        //FIXME:   need to assign

        Childs[index + 1] = nullptr; // this child will link to another node

        keysCount++;
        return index;
    }
}

template <typename KeyType>
position TreeNode<KeyType>::addKey(const KeyType& key, offsetNumber val)
{
    if (!isLeaf)
    {
        cout << "Error:addKey(KeyType &key,offsetNumber val) is a function for leaf nodes" << endl;
        return -1;
    }
    if (keysCount == 0)
    {
        Keys[0] = key;
        Values[0] = val;
        keysCount++;
        return 0;
    }
    //numberOfKeys > 0
    //和上面的差不多
    position index = 0; // record the index of the tree
    bool exist = search(key, index);
    if (exist)
    {
        cout << "Error:In addKey(Keytype &key, offsetNumber val),key has already in the tree!" << endl;
        exit(3); //TODO
    }
    else // addKey the key into the node
    {
        for (size_t i = keysCount; i > index; i--)
        {
            Keys[i] = Keys[i - 1];
            Values[i] = Values[i - 1];
        }
        Keys[index] = key;
        Values[index] = val;
        keysCount++;
        return index;
    }
}

template <typename KeyType>
TreeNode<KeyType>* TreeNode<KeyType>::splite(KeyType& key)
{
    position middleNode = (degree - 1) / 2;
    TreeNode* newNode = new TreeNode(degree, isLeaf);
    if (newNode == nullptr)
    {
        cout << "Problems in allocate momeory of TreeNode in splite node of " << key << endl;
        exit(2); //TODO:
    }
    if (isLeaf)
    {
        key = Keys[middleNode + 1]; //return bt reference

        //copy the right half part of Keys to newNode
        for (size_t i = middleNode + 1; i < degree; i++)
        {
            newNode->Keys[i - middleNode - 1] = Keys[i];
            Keys[i] = KeyType();
            newNode->Values[i - middleNode - 1] = Values[i];
            Values[i] = offsetNumber();
        }
    }
}

//----------------------------------------------------------

template <typename KeyType>
BPlusTree<KeyType>::BPlusTree(string fileName, int keySize, int degree)
{
    this->degree = degree;
    this->file = nullptr;
    this->fileName = fileName;
    this->keysCount = 0;
    this->keySize = keySize;
    this->leafHead = nullptr;
    this->level = 0;
    this->nodeCount = 0;
    this->root = nullptr;
    init_tree();
    readFromDisk(); //read all from disk
}
template <typename KeyType>
BPlusTree<KeyType>::~BPlusTree()
{
    dropTree(root);
    keysCount = 0;
    level = 0;
    root = nullptr;
}

template <typename KeyType>
void BPlusTree<KeyType>::init_tree()
{
    root = new TreeNode<KeyType>(degree, true);
    keysCount = 0;
    leafHead = root;
    level = 1;
    nodeCount = 1;
}
template <typename KeyType>
void BPlusTree<KeyType>::findToLeaf(NodePtr pNode, KeyType key, searchNodeParse& snp)
{
    position index = 0;
    bool isFound = pNode->search(key, index); //NOTE: index changed by reference
    if (isFound)
    {
        if (pNode->isLeaf)
        {
            snp.index = index;
            snp.isFound = true;
            snp.pNode = pNode;
        }
        else // if found but not a leaf,
        {
            pNode = pNode->Childs[index + 1];

            while (!pNode->isLeaf)
            {
                pNode = pNode->Childs[0];
            }
            snp.index = 0;
            snp.isFound = true;
            snp.pNode = pNode;
        }
    }
    else
    {
        if (pNode->isLeaf)
        {
            snp.pNode = pNode;
            snp.index = index;
            snp.isFound = false;
        }
        else
        {
            //NOTE: 递归搜索
            findToLeaf(pNode->Childs[index], key, snp);
        }
    }
}

template <typename KeyType>
bool BPlusTree<KeyType>::insertKey(const KeyType& key, const offsetNumber val)
{
    if (root == nullptr)
    {
        init_tree();
    }
    searchNodeParse snp;
    findToLeaf(root, key, snp);
    if (snp.isFound)
    {
        cout << "Error:in insert key to index: the duplicated key!" << endl;
        return false;
    }
    else
    {
        snp.pNode->addKey(key, val);
        if (snp.pNode->keysCount == degree)
        {
            adjustAfterInsert(snp.pNode);
        }
        keysCount++;
        return true;
    }
}


template <typename KeyType>
bool BPlusTree<KeyType>::adjustAfterInsert(NodePtr pNode)
{
    KeyType key;
	NodePtr newNode = NULL;
	newNode = pNode->splite(key);
    nodeCount++;

    if (pNode->isRoot()) // the node is the root
    {
        NodePtr root = new TreeNode<KeyType>(degree, false);
        if (root == nullptr)
        {
            cout << "Error: can not allocate memory for the new root in adjustAfterInsert" << endl;
            exit(1);
        }
        else
        {
            level++;
            nodeCount++;
            this->root = root;
            pNode->parent = root;
            newNode->parent = root;
            root->addKey(key);
            root->Childs[0] = pNode;
            root->Childs[1] = newNode;
            return true;
        }
    }    // end root
    else // if it is not the root
    {
        NodePtr parent = pNode->parent;
        position index = parent->addKey(key);

        parent->Childs[index + 1] = newNode;
        newNode->parent = parent;
        if (parent->keysCount == degree)
            return adjustAfterInsert(parent);

        return true;
    }
}


template <typename KeyType>
offsetNumber BPlusTree<KeyType>::search(const KeyType& key)
{
    if (root == nullptr)
    {
        return -1;
    }
    searchNodeParse snp;
    findToLeaf(root, key, snp);
    if (snp.isFound)
    {
        return snp.pNode->Values[snp.index];
    }
    else
    {
        return -1;
    }
}

template <typename KeyType>
bool BPlusTree<KeyType>::deleteKey(KeyType& key)
{
    if (root == nullptr)
    {
        cout << "ERROR: In deleteKey, no nodes in the tree " << fileName << "!" << endl;
        return false;
    }
    searchNodeParse snp;
    findToLeaf(root, key, snp);

    if (snp.isFound)
    {
        if (snp.pNode->isRoot()) //既是叶节点也是根节点
        {
            snp.pNode->removeAt(snp.index);
            keysCount--;
        }
        //TODO:  这里有一个问题！！！leafHead 没有被更新过！

        else if (snp.index == 0 && leafHead != snp.pNode) // the key exist in the branch.
        {
            // go to upper level to update the branch level
            position index = 0;
            NodePtr nowParent = snp.pNode->parent;
            bool isFoundInBranch = nowParent->search(key, index);
            while (!isFoundInBranch)
            {
                if (nowParent->parent != nullptr)
                    nowParent = nowParent->parent;
                else
                {
                    break;
                }
                isFoundInBranch = nowParent->search(key, index);
            } // end of search in the branch
            nowParent->Keys[index] = snp.pNode->Keys[1];//NOTE: 由叶节点第二个补上
            snp.pNode->removeAt(snp.index);
            keysCount--;
        }
        else //this key must just exist in the leaf too.
        {
            snp.pNode->removeAt(snp.index);
            keysCount--;
        }

        return adjustAfterDelete(snp.pNode);
    }
    else //not found
    {
        cout << "ERROR: In deleteKey, no keys in the tree " << fileName << "!" << endl;
        return false;
    }
}

template <typename KeyType>
bool BPlusTree<KeyType>::adjustAfterDelete(NodePtr pNode)
{
    //TODO:?  不理解==??????
    position middleKey = (degree - 1) / 2;
    if (false
        || ((pNode->isLeaf) && (pNode->keysCount >= middleKey))
        || ((degree != 3) && (!pNode->isLeaf) && (pNode->keysCount >= middleKey - 1))//?
        || ((degree == 3) && (!pNode->isLeaf) && (pNode->keysCount < 0))//?
        ) // do not need to adjust
    {
        return true;
    }
    if (pNode->isRoot())
    {
        if (pNode->keysCount > 0) //do not need to adjust
        {
            return true;
        }
        else if (root->isLeaf) //the true will be an empty tree
        {
            delete pNode;
            root = nullptr;
            leafHead = nullptr;
            level--;
            nodeCount--;
        }
        else // root will be the leafhead
        {
            root = pNode->Childs[0];
            root->parent = nullptr;
            delete pNode;
            level--;
            nodeCount--;
        }
        return false;
    } // end root
    NodePtr parent = pNode->parent, brother = nullptr;
    if (pNode->isLeaf)
    {
        position index = 0;
        parent->search(pNode->Keys[0], index);

        if (true
            && (parent->Childs[0] != pNode)
            && (index + 1 == parent->keysCount)
            )//被删除后的节点为叶节点，但不在叶节点的最左侧 
            //choose the left brother to merge or replace
        {
            brother = parent->Childs[index];//将index的最右边和index+1结合
            // choose the most right key of brother to addKey to the left hand of the pnode
            if (brother->keysCount > middleKey)
            {
                for (size_t i = pNode->keysCount; i > 0; i--)//将右边结点右移腾出空间
                {
                    pNode->Keys[i] = pNode->Keys[i - 1];
                    pNode->Values[i] = pNode->Values[i - 1];
                }
                pNode->Keys[0] = brother->Keys[brother->keysCount - 1];//插入
                pNode->Values[0] = brother->Values[brother->keysCount - 1];
                brother->removeAt(brother->keysCount - 1);
                pNode->keysCount++;
                parent->Keys[index] = pNode->Keys[0];
                return true;
            }    // end addKey
            else //等于最小keysCount，直接合并两个结点
                //merge the node with its brother
            {
                parent->removeAt(index);//删除结点pNode，同时删除两结点中间的index 

                for (int i = 0; i < pNode->keysCount; i++)
                {
                    brother->Keys[i + brother->keysCount] = pNode->Keys[i];
                    brother->Values[i + brother->keysCount] = pNode->Values[i];
                }
                brother->keysCount += pNode->keysCount;
                brother->nextLeaf = pNode->nextLeaf;

                delete pNode;
                nodeCount--;

                return adjustAfterDelete(parent);
            } // end merge

        }    // end of the left brother
        else // choose the right brother
        {
            if (parent->Childs[0] == pNode)
            {
                brother = parent->Childs[1];
            }
            else
            {
                brother = parent->Childs[index + 2];
            }
            // choose the most left key of brother to addKey to the right hand of the node
            if (brother->keysCount > middleKey)
            {//合并到左边的pNode，更新
                pNode->Keys[pNode->keysCount] = brother->Keys[0];
                pNode->Values[pNode->keysCount] = brother->Values[0];
                pNode->keysCount++;
                brother->removeAt(0);
                if (parent->Childs[0] == pNode)
                {
                    parent->Keys[0] = brother->Keys[0];
                }
                else
                {
                    parent->Keys[index + 1] = brother->Keys[0];//更新指向pNode 的parent的key
                }
                return true;

            }    // end addKey
            else //brother有最小临界结点数 merge the node with its brother
            {
                for (int i = 0; i < brother->keysCount; i++)
                {
                    pNode->Keys[pNode->keysCount + i] = brother->Keys[i];
                    pNode->Values[pNode->keysCount + i] = brother->Values[i];
                }
                if (pNode == parent->Childs[0])

                {
                    parent->removeAt(0);
                }
                else
                {
                    parent->removeAt(index + 1);
                }
                pNode->keysCount += brother->keysCount;
                pNode->nextLeaf = brother->nextLeaf;
                delete brother;
                nodeCount--;

                return adjustAfterDelete(parent);
            } //end merge
        }     // end of the right brother

    }    // end leaf
    else // branch node
    {
        size_t index = 0;
        parent->search(pNode->Childs[0]->Keys[0], index);
        // choose the left brother to merge or replace
        if (true
            && (parent->Childs[0] != pNode)
            && (index + 1 == parent->keysCount)
            )
        {
            brother = parent->Childs[index];
            // choose the most right key and child to addKey to the left hand of the pnode
            if (brother->keysCount > middleKey - 1)
            {
                //modify the pnode
                pNode->Childs[pNode->keysCount + 1] = pNode->Childs[pNode->keysCount];
                for (size_t i = pNode->keysCount; i > 0; i--)
                {
                    pNode->Childs[i] = pNode->Childs[i - 1];
                    pNode->Keys[i] = pNode->Keys[i - 1];
                }
                pNode->Childs[0] = brother->Childs[brother->keysCount];
                pNode->Keys[0] = parent->Keys[index];
                pNode->keysCount++;
                //modify the father
                parent->Keys[index] = brother->Keys[brother->keysCount - 1];
                //modify the brother and child
                if (brother->Childs[brother->keysCount])
                {
                    brother->Childs[brother->keysCount]->parent = pNode;
                }
                brother->removeAt(brother->keysCount - 1);

                return true;

            }    // end addKey
            else // merge the node with its brother
            {
                //modify the brother and child
                brother->Keys[brother->keysCount] = parent->Keys[index];
                parent->removeAt(index);
                brother->keysCount++;

                for (int i = 0; i < pNode->keysCount; i++)
                {
                    brother->Childs[brother->keysCount + i] = pNode->Childs[i];
                    brother->Keys[brother->keysCount + i] = pNode->Keys[i];
                    brother->Childs[brother->keysCount + i]->parent = brother;
                }
                brother->Childs[brother->keysCount + pNode->keysCount] = pNode->Childs[pNode->keysCount];
                brother->Childs[brother->keysCount + pNode->keysCount]->parent = brother;

                brother->keysCount += pNode->keysCount;

                delete pNode;
                nodeCount--;

                return adjustAfterDelete(parent);
            }

        }    // end of the left brother
        else // choose the right brother
        {
            if (parent->Childs[0] == pNode)
            {
                brother = parent->Childs[1];
            }
            else
            {
                brother = parent->Childs[index + 2];
            }
            if (brother->keysCount > middleKey - 1) // choose the most left key and child to addKey to the right hand of the pnode
            {
                //modifty the pnode and child
                pNode->Childs[pNode->keysCount + 1] = brother->Childs[0];
                pNode->Keys[pNode->keysCount] = brother->Keys[0];
                pNode->Childs[pNode->keysCount + 1]->parent = pNode;
                pNode->keysCount++;
                //modify the fater
                if (pNode == parent->Childs[0])
                {
                    parent->Keys[0] = brother->Keys[0];
                }
                else
                {
                    parent->Keys[index + 1] = brother->Keys[0];
                }
                //modify the brother
                brother->Childs[0] = brother->Childs[1];
                brother->removeAt(0);

                return true;
            }
            else // merge the node with its brother
            {
                //modify the pnode and child
                pNode->Keys[pNode->keysCount] = parent->Keys[index];

                if (pNode == parent->Childs[0])
                {
                    parent->removeAt(0);
                }
                else
                {
                    parent->removeAt(index + 1);
                }
                pNode->keysCount++;

                for (int i = 0; i < brother->keysCount; i++)
                {
                    pNode->Childs[pNode->keysCount + i] = brother->Childs[i];
                    pNode->Keys[pNode->keysCount + i] = brother->Keys[i];
                    pNode->Childs[pNode->keysCount + i]->parent = pNode;
                }
                pNode->Childs[pNode->keysCount + brother->keysCount] = brother->Childs[brother->keysCount];
                pNode->Childs[pNode->keysCount + brother->keysCount]->parent = pNode;

                pNode->keysCount += brother->keysCount;

                delete brother;
                nodeCount--;

                return adjustAfterDelete(parent);
            }
        }
    }

    return false;
}
template <typename KeyType>
void BPlusTree<KeyType>::dropTree(NodePtr node)
{
    if (!node)
        return;
    if (!node->isLeaf) //if it has child
    {
        for (size_t i = 0; i <= node->keysCount; i++)
        {
            dropTree(node->Childs[i]);
            node->Childs[i] = nullptr;
        }
    }
    delete node;
    nodeCount--;
    return;
}


template <typename KeyType>
void BPlusTree<KeyType>::readFromDisk()
{
    file = bm.getFile(fileName.c_str());
    blockNode* btmp = bm.getBlockHead(file);
    while (btmp != nullptr)
    {
        readFromDisk(btmp);
        if (btmp->ifbottom)
            break;
        btmp = bm.getNextBlock(file, btmp);
    }
}


template <typename KeyType>
void BPlusTree<KeyType>::readFromDisk(blockNode* btmp)
{
    int valueSize = sizeof(offsetNumber);
    char* indexBegin = bm.getContent(*btmp);
    char* valueBegin = indexBegin + keySize;
    KeyType key;
    offsetNumber value;

    while (valueBegin - bm.getContent(*btmp) < bm.getUsingSize(*btmp))
        // there are available position in the block
    {
        key = *(KeyType*)indexBegin;
        value = *(offsetNumber*)valueBegin;
        insertKey(key, value);
        valueBegin += keySize + valueSize;
        indexBegin += keySize + valueSize;
    }
}


template <typename KeyType>
void BPlusTree<KeyType>::writeBackToDisk()
{
    blockNode* btmp = bm.getBlockHead(file);
    NodePtr ntmp = leafHead;
    int valueSize = sizeof(offsetNumber);
    while (ntmp != nullptr)
    {
        bm.setUsingSize(*btmp, 0);//将当前块地址置为0，清空
        bm.setDirty(*btmp);
        for (int i = 0; i < ntmp->keysCount; i++)//对于每个keySize大小的数据，将其拷贝在原内存之后
        {
            char* key = (char*) & (ntmp->Keys[i]);
            char* value = (char*) & (ntmp->Values[i]);
            memcpy(bm.getContent(*btmp) + bm.getUsingSize(*btmp), key, keySize);
            bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) + keySize);
            memcpy(bm.getContent(*btmp) + bm.getUsingSize(*btmp), value, valueSize);
            bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) + valueSize);
        }

        btmp = bm.getNextBlock(file, btmp);
        ntmp = ntmp->nextLeaf;
    }
    while (1) // delete the empty node
    {
        if (btmp->ifbottom)
            break;
        bm.setUsingSize(*btmp, 0);
        bm.setDirty(*btmp);
        btmp = bm.getNextBlock(file, btmp);
    }
}
