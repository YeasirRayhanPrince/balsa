# NTP-QO Development Todos

## Progress Status

### ✅ Completed
- [x] Create clean production-style project structure
- [x] Implement core AST nodes (adapt from Balsa's Node class)
- [x] Create database connectors for multi-DB plan extraction

### 🚧 In Progress
- [ ] Implement plan tokenizer for transformer input

### 📋 Pending
- [ ] Build transformer model for next token prediction
- [ ] Create training pipeline and data collection
- [ ] Implement main QueryOptimizer interface

## Detailed Task Breakdown

### 1. Plan Tokenizer ⏳
**Status**: In Progress
**Description**: Create a tokenizer that converts query plan trees into token sequences suitable for transformer models.

**Key Features Needed**:
- Vocabulary management for operators, tables, and special tokens
- Tree-to-sequence conversion (pre-order, post-order, or custom traversal)
- Handling of partial plans for next token prediction
- Support for multiple database systems

### 2. Transformer Model 📋
**Status**: Pending
**Description**: Build a transformer-based model for next token prediction in query plans.

**Key Features Needed**:
- GPT-style decoder architecture
- Plan-aware attention mechanisms
- Multi-database embedding layers
- Beam search for plan generation

### 3. Training Pipeline 📋
**Status**: Pending
**Description**: Create training infrastructure for collecting data and training models.

**Key Features Needed**:
- Multi-database plan collection
- Data preprocessing and augmentation
- Training loop with validation
- Experiment tracking (wandb integration)

### 4. Query Optimizer Interface 📋
**Status**: Pending
**Description**: Main interface for using the trained model to optimize queries.

**Key Features Needed**:
- Query parsing and analysis
- Plan generation using trained model
- Cost estimation and ranking
- Integration with existing database systems

## Technical Decisions Made

1. **Architecture**: Clean, production-style structure (not over-engineered)
2. **AST Design**: Extended Balsa's Node class with multi-DB and tokenization support
3. **Database Support**: PostgreSQL (full), MySQL (partial), HyPer/Umbra (placeholders)
4. **Tokenization Strategy**: Tree-to-sequence with operator and table tokens

## Next Steps

1. Complete the plan tokenizer implementation
2. Set up basic transformer model structure
3. Create simple training pipeline for single database
4. Extend to multi-database training
5. Build evaluation framework

## Notes

- Code follows industry ML practices (flat structure, clear modules)
- Maintains compatibility with Balsa where possible
- Designed for extensibility to new database systems
- Focus on next token prediction instead of cost regression

---
*Last updated: 2024-09-16*