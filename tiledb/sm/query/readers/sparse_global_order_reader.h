/**
 * @file   sparse_global_order_reader.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines class SparseGlobalOrderReader.
 */

#ifndef TILEDB_SPARSE_GLOBAL_ORDER_READER
#define TILEDB_SPARSE_GLOBAL_ORDER_READER

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_coords.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
struct PreprocessTileMergeFuture;
struct RangeLowerBound;

enum class AddNextCellResult {
  // finished the current tile
  Done,
  // successfully added a cell to the queue
  FoundCell,
  // more tiles from the same fragment are needed to continue
  NeedMoreTiles,
  // this tile cannot continue because it would be out of order with
  // un-created result tiles
  MergeBound
};

/**
 * Identifies an order in which to load result tiles.
 * See `preprocess_tile_order_`.
 */
struct PreprocessTileOrder {
  bool enabled_;
  size_t cursor_;
  std::vector<ResultTileId> tiles_;

  bool has_more_tiles() const {
    return enabled_ && cursor_ < tiles_.size();
  }

  /**
   * Identifies the current position in the preprocess tile stream using
   * the read state, and updates the cursor to that position.
   * This is called after starting the result tile order.
   *
   * When running libtiledb natively, this is only called in the
   * first instance of `tiledb_query_submit` and sets the cursor
   * to that position.
   *
   * When running libtiledb against the REST server, this is called
   * on the REST server for each `tiledb_query_submit`.
   * We assume that recomputing the tile order for each message
   * is cheaper than serializing the tile order after computing it once.
   * However, as the read state progresses over the subarray,
   * the tiles which qualify as input to the tile merge change.
   * This causes the tile list to vary from submit to submit.
   * Hence instead of serializing the position in the list we must
   * recompute it.
   */
  template <typename MergeFuture>
  static uint64_t compute_cursor_from_read_state(
      const RelevantFragments& relevant_fragments,
      const std::span<const FragIdx>& read_state,
      const std::span<const ResultTileId>& tiles,
      MergeFuture& merge_future) {
    // The current position is that of the first tile in the list
    // which comes after the last tile in the list from which
    // data was emitted.
    //
    // Data was emitted from a tile if its `cell_idx_` is nonzero.
    //
    // In a synchronous world we can identify that tile trivially by
    // walking backwards from the end of the list and finding
    // the first nonzero `cell_idx_`.
    //
    // In an async world we want to walk forwards, so that we
    // don't have to wait for the whole merge to finish.

    size_t bound = 0;
    for (const auto f : relevant_fragments) {
      std::optional<uint64_t> f_bound;
      for (uint64_t t = 0; t < tiles.size(); t++) {
        merge_future.wait_for(t);

        if (tiles[t].fragment_idx_ != f) {
          continue;
        }
        if (tiles[t].tile_idx_ < read_state[f].tile_idx_) {
          f_bound.emplace(t + 1);
        } else if (tiles[t].tile_idx_ == read_state[f].tile_idx_) {
          if (read_state[f].cell_idx_ > 0) {
            // this is the current tile, we have emitted some cells already
            f_bound.emplace(t + 1);
          } else if (f_bound.has_value()) {
            // we exhausted the previous tile but did not emit anything from
            // this one
          } else {
            // this is the first tile from the fragment, with no data emitted
          }
          break;
        } else {
          // this means we never saw the `==` tile, it did not qualify
          // (is this even reachable except for the end of the fragment?)
          // but the read state had advanced beyond the previous, so that's the
          // bound
          if (f_bound.has_value()) {
            f_bound.emplace(f_bound.value() + 1);
          }
          break;
        }
      }
      if (f_bound.has_value() && f_bound.value() > bound) {
        bound = f_bound.value();
      }
    }

    return bound;
  }
};

/** Processes sparse global order read queries. */

template <class BitmapType>
class SparseGlobalOrderReader : public SparseIndexReaderBase,
                                public IQueryStrategy {
 public:
  typedef std::list<GlobalOrderResultTile<BitmapType>> ResultTilesList;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseGlobalOrderReader(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StrategyParams& params,
      bool consolidation_with_timestamps);

  /** Destructor. */
  ~SparseGlobalOrderReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(SparseGlobalOrderReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SparseGlobalOrderReader);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Finalizes the reader.
   *
   * @return Status.
   */
  Status finalize() override {
    return Status::Ok();
  }

  /**
   * Returns `true` if the query was incomplete.
   *
   * @return The query status.
   */
  bool incomplete() const override;

  /**
   * Returns `true` if the query was incomplete.
   *
   * @return The query status.
   */
  QueryStatusDetailsReason status_incomplete_reason() const override;

  /**
   * Initialize the memory budget variables.
   */
  void refresh_config() override;

  /**
   * Performs a read query using its set members.
   *
   * @return Status.
   */
  Status dowork() override;

  /** Resets the reader object. */
  void reset() override;

  /** Returns the name of the strategy */
  std::string name() override;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /**
   * State for the optional mode to preprocess the tiles across
   * all fragments and merge them into a single list which identifies
   * the order they should be read in.
   *
   * This is used to merge the tiles
   * into a single globally-ordered list prior to loading.
   * Tile identifiers in this list are sorted using their starting ranges
   * and have already had the subarray (if any) applied.
   */
  PreprocessTileOrder preprocess_tile_order_;

  /**
   * Result tiles currently for which we loaded coordinates but couldn't
   * process in the previous iteration.
   */
  std::vector<ResultTilesList> result_tiles_leftover_;

  /**
   * State for the mode to evenly distribute memory
   * budget amongst the fragments and create a result
   * tile per fragment regardless of how their tiles
   * fit in the unified global order.
   *
   * Used only when preprocess tile order is not enabled.
   */
  struct {
    /** Memory used for coordinates tiles per fragment */
    std::vector<uint64_t> memory_used_for_coords_;
    /** Memory budget per fragment */
    double per_fragment_memory_;
  } per_fragment_memory_state_;

  /** Enables consolidation with timestamps or not. */
  bool consolidation_with_timestamps_;

  /** Mutex to protect the tile queue. */
  std::mutex tile_queue_mutex_;

  /** Stores last cell for fragments consolidated with timestamps. */
  std::vector<FragIdx> last_cells_;

  // Are we doing purge deletes consolidation. The consolidation with
  // timestamps flag will be set and we will have a post query condition
  // bitmap. The later is only true in consolidation when delete conditions
  // are present.
  bool purge_deletes_consolidation_;

  // For purge deletes consolidation and no duplicates, we read in a different
  // mode. We will first sort cells in the tile queue with the same coordinates
  // using timestamps (where the cell with the greater timestamp comes first).
  // Then when adding cells for a fragment consolidated with timestamps, we
  // will add all the dups at once. Finally, when creating cell slabs, we will
  // stop creating cell slabs once a cell is deleted. This will enable cells
  // created after the last delete time to go through, but the cells created
  // before to be purged.
  bool purge_deletes_no_dups_mode_;

  /** Are tile offsets loaded? */
  bool tile_offsets_loaded_;

  /* ********************************* */
  /*       PRIVATE DECLARATIONS        */
  /* ********************************* */

  /** Tile min heap. */
  template <typename CompType>
  using TileMinHeap = std::priority_queue<
      GlobalOrderResultCoords<BitmapType>,
      std::vector<GlobalOrderResultCoords<BitmapType>>,
      CompType>;

  /** Tile list iterator. */
  using TileListIt = typename ResultTilesList::iterator;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Load all tile offsets required for the read operation. */
  void load_all_tile_offsets();

  /**
   * Get the coordinate tiles size for a dimension.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   *
   * @return Tiles size.
   */
  uint64_t get_coord_tiles_size(unsigned dim_num, unsigned f, uint64_t t);

  /**
   * Add a result tile to process, making sure maximum budget is respected.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   * @param frag_md Fragment metadata.
   * @param result_tiles Result tiles per fragment.
   *
   * @return buffers_full.
   */
  bool add_result_tile(
      const unsigned dim_num,
      const unsigned f,
      const uint64_t t,
      const FragmentMetadata& frag_md,
      std::vector<ResultTilesList>& result_tiles);

  /**
   * Kicks off computation of the single list of tiles across all fragments
   * arranged in the order they must be processed for this query.
   * See `preprocess_tile_order_`.
   *
   * The `merge_future` in-out parameter provides memory locations for
   * the parallel merge algorithm inputs - this includes the global order
   * comparator as well as the fragment `ResultTileId` lists. This method sets
   * the parallel merge algorithm future on `merge_future` as its postcondition.
   *
   * The merge output is written to `this->preprocess_tile_order_.tiles_`
   * and the `merge_future` can be used to poll the state of this asynchronous
   * operation.
   *
   * @param [in-out] merge_future where to put data for the merge and the merge
   *
   * @precondition `merge_future` is freshly constructed.
   * @postcondition `merge_future.fragment_result_tiles_` is initialized with
   * the lists of tiles from each fragment which qualify for the subarray.
   * @postcondition `merge_future.cmp_` holds the memory for the comparator used
   * in the asynchronous parallel merge.
   * @postcondition `merge_future.merge_` is initialized and can be used to poll
   * the results of the asynchronous parallel merge.
   */
  void preprocess_compute_result_tile_order(
      PreprocessTileMergeFuture& merge_future);

  /**
   * Create the result tiles.
   *
   * @param result_tiles Result tiles per fragment.
   * @param preprocess_future future for polling the global order tile
   * stream, if running in preprocess mode
   *
   * @return Newly created tiles.
   */
  std::vector<ResultTile*> create_result_tiles(
      std::vector<ResultTilesList>& result_tiles,
      std::optional<PreprocessTileMergeFuture>& preprocess_future);

  /**
   * Create the result tiles naively, without coordinating
   * the ranges of tiles of each fragment. This uses a
   * per-fragment memory budget, and (assuming enough memory)
   * creates at least one result tile for each fragment.
   *
   * See `all_fragment_tile_order_`.
   *
   * @param result_tiles [in-out] Result tiles per fragment.
   */
  void create_result_tiles_all_fragments(
      std::vector<ResultTilesList>& result_tiles);

  /**
   * Create the result tiles using the pre-processed
   * tile order. See `preprocess_tile_order_`.
   *
   * @param result_tiles [in-out] Result tiles per fragment.
   * @param merge_future handle to polling the preprocess merge stream
   *                     (if it has not completed yet)
   */
  void create_result_tiles_using_preprocess(
      std::vector<ResultTilesList>& result_tiles,
      std::optional<PreprocessTileMergeFuture>& merge_future);

  /**
   * Clean tiles that have 0 results from the tile lists.
   *
   * @param result_tiles Result tiles vector.
   */
  void clean_tile_list(std::vector<ResultTilesList>& result_tiles);

  /**
   * Process tiles with timestamps to deduplicate entries.
   *
   * @param result_tiles Result tiles to process.
   */
  void dedup_tiles_with_timestamps(std::vector<ResultTile*>& result_tiles);

  /**
   * Process fragments with timestamps to deduplicate entries.
   * This removes cells across tiles.
   *
   * @param result_tiles Result tiles per fragment.
   */
  void dedup_fragments_with_timestamps(
      std::vector<ResultTilesList>& result_tiles);

  /**
   * Compute the number of cells possible to merge from user buffers.
   *
   * @return Number of cells possible to merge from user buffers.
   */
  uint64_t max_num_cells_to_copy();

  /**
   * Is the result coord the last cell of a consolidated fragment with
   * timestamps.
   *
   * @param frag_idx Fragment index for the result coords.
   * @param rc Result coords.
   * @param result_tiles Result tiles per fragment.
   *
   * @return true if the result coords is the last cell of a consolidated
   * fragment with timestamps.
   */
  inline bool last_in_memory_cell_of_consolidated_fragment(
      const unsigned int frag_idx,
      const GlobalOrderResultCoords<BitmapType>& rc,
      const std::vector<ResultTilesList>& result_tiles) const {
    return !tmp_read_state_.all_tiles_loaded(frag_idx) &&
           fragment_metadata_[frag_idx]->has_timestamps() &&
           rc.tile_ == &result_tiles[frag_idx].back() &&
           rc.tile_->tile_idx() == last_cells_[frag_idx].tile_idx_ &&
           rc.pos_ == last_cells_[frag_idx].cell_idx_;
  }

  /**
   * Add, for a fragment with timestamps, all duplicates of a certain cell.
   *
   * @param rc Current result coords for the fragment.
   * @param result_tiles_it Iterator, per frag, in the list of retult tiles.
   * @param result_tiles Result tiles per fragment.
   * @param tile_queue Queue of one result coords, per fragment, sorted.
   * @param to_delete List of tiles to delete.
   *
   * @return If more tiles are needed.
   */
  template <class CompType>
  bool add_all_dups_to_queue(
      GlobalOrderResultCoords<BitmapType>& rc,
      std::vector<TileListIt>& result_tiles_it,
      const std::vector<ResultTilesList>& result_tiles,
      TileMinHeap<CompType>& tile_queue,
      std::vector<TileListIt>& to_delete);

  /**
   * Add a cell (for a specific fragment) to the queue of cells currently being
   * processed.
   *
   * @param rc Current result coords for the fragment.
   * @param result_tiles_it Iterator, per frag, in the list of retult tiles.
   * @param result_tiles Result tiles per fragment.
   * @param tile_queue Queue of one result coords, per fragment, sorted.
   * @param to_delete List of tiles to delete.
   * @param merge_bound reference to the bound where results can be correctly
   * put in the queue
   *
   * @return result of trying to add a cell
   */
  template <class CompType>
  AddNextCellResult add_next_cell_to_queue(
      GlobalOrderResultCoords<BitmapType>& rc,
      std::vector<TileListIt>& result_tiles_it,
      const std::vector<ResultTilesList>& result_tiles,
      TileMinHeap<CompType>& tile_queue,
      std::vector<TileListIt>& to_delete,
      const std::optional<RangeLowerBound>& merge_bound);

  /**
   * Computes a tile's Hilbert values for a tile.
   *
   * @param result_tiles Result tiles to process.
   */
  void compute_hilbert_values(std::vector<ResultTile*>& result_tiles);

  /**
   * Update the fragment index to the larger between current one and the one
   * passed in.
   *
   * @param tile Current tile.
   * @param c Current cell index.
   */
  void update_frag_idx(GlobalOrderResultTile<BitmapType>* tile, uint64_t c);

  /**
   * Compute the result cell slabs once tiles are loaded.
   *
   * @param num_cells Number of cells that can be copied in the user buffer.
   * @param result_tiles Result tiles per fragment.
   * @param merge_future handle to polling the preprocess merge stream
   *                     (if it has not completed yet).
   *
   * @return user_buffers_full, result_cell_slabs.
   */
  template <class CompType>
  tuple<bool, std::vector<ResultCellSlab>> merge_result_cell_slabs(
      uint64_t num_cells,
      std::vector<ResultTilesList>& result_tiles,
      std::optional<PreprocessTileMergeFuture>& merge_future);

  /**
   * Compute parallelization parameters for a tile copy operation.
   *
   * @param range_thread_idx Current range thread index.
   * @param num_range_threads Total number of range threads.
   * @param start Start cell position to process.
   * @param lenth Number of cells to process.
   * @param cell_offsets Cell offset per result tile.
   *
   * @return min_pos, max_pos, dest_cell_offset, skip_copy.
   */
  tuple<uint64_t, uint64_t, uint64_t, bool> compute_parallelization_parameters(
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads,
      const uint64_t start,
      const uint64_t length,
      const uint64_t cell_offset);

  /**
   * Copy offsets tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param nullable Is this field nullable.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   * @param var_data Stores pointers to var data cell values.
   */
  template <class OffType>
  void copy_offsets_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool nullable,
      const OffType offset_div,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<const void*>& var_data);

  /**
   * Copy var data tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param var_buffer_size Size of the var data buffer.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   * @param var_data Stores pointers to var data cell values.
   */
  template <class OffType>
  void copy_var_data_tiles(
      const uint64_t num_range_threads,
      const OffType offset_div,
      const uint64_t var_buffer_size,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<const void*>& var_data);

  /**
   * Copy fixed size data tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param is_dim Is this field a dimension.
   * @param nullable Is this field nullable.
   * @param dim_idx Dimention index, used for zipped coords.
   * @param cell_size Cell size.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   */
  void copy_fixed_data_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool is_dim,
      const bool nullable,
      const unsigned dim_idx,
      const uint64_t cell_size,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Copy timestamps tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   */
  void copy_timestamps_tiles(
      const uint64_t num_range_threads,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Copy delete metadata tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   */
  void copy_delete_meta_tiles(
      const uint64_t num_range_threads,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Make sure we respect memory budget for copy operation by making sure that,
   * for all attributes to be copied, the size of tiles in memory can fit into
   * the budget.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_cell_slabs Result cell slabs to process, might be truncated.
   * @param user_buffers_full Boolean that indicates if the user buffers are
   * full or not. If this comes in as `true`, it might be reset to `false` if
   * the results were truncated.
   *
   * @return total_mem_usage_per_attr.
   */
  std::vector<uint64_t> respect_copy_memory_budget(
      const std::vector<std::string>& names,
      std::vector<ResultCellSlab>& result_cell_slabs,
      bool& user_buffers_full);

  /**
   * Compute the var size offsets and make sure all the data can fit in the
   * user buffer.
   *
   * @param stats Stats.
   * @param result_cell_slabs Result cell slabs to process, might be truncated.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return caused_overflow, new_var_buffer_size.
   */
  template <class OffType>
  tuple<bool, uint64_t> compute_var_size_offsets(
      stats::Stats* stats,
      std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Get the sorted unique result tile list from the result cell slabs.
   *
   * @param result_cell_slabs Result cell slabs.
   * @param aggregate_only Are we generating the list for aggregate only fields?
   * @return vector of result tiles.
   */
  std::vector<ResultTile*> result_tiles_to_load(
      std::vector<ResultCellSlab>& result_cell_slabs, bool aggregate_only);

  /**
   * Copy cell slabs.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_cell_slabs The result cell slabs to process.
   * @param user_buffers_full Boolean that indicates if the user buffers are
   * full or not.
   */
  template <class OffType>
  void process_slabs(
      std::vector<std::string>& names,
      std::vector<ResultCellSlab>& result_cell_slabs,
      bool& user_buffers_full);

  /**
   * Copy tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param name Field to copy.
   * @param is_dim Is the field a dimension.
   * @param cell_offsets Cell offset per result tile.
   * @param result_cell_slabs The result cell slabs to process.
   * @param last_field_to_overflow Last field that caused an overflow.
   *
   * @return user_buffers_full.
   */
  template <class OffType>
  bool copy_tiles(
      const uint64_t num_range_threads,
      const std::string name,
      const bool is_dim,
      std::vector<uint64_t>& cell_offsets,
      std::vector<ResultCellSlab>& result_cell_slabs,
      std::optional<std::string>& last_field_to_overflow);

  /**
   * Make an aggregate buffer.
   *
   * @param name Field to aggregate.
   * @param var_sized Is the field var sized?
   * @param nullable Is the field nullable?
   * @param cell_size Cell size.
   * @param min_cell Min cell to aggregate.
   * @param min_cell Max cell to aggregate.
   * @param rt Result tile.
   */
  AggregateBuffer make_aggregate_buffer(
      const std::string name,
      const bool var_sized,
      const bool nullable,
      const uint64_t cell_size,
      const uint64_t min_cell,
      const uint64_t max_cell,
      ResultTile& rt);

  /**
   * Returns wether or not we can aggregate the tile with only the fragment
   * metadata.
   *
   * @param rcs Result cell slab.
   * @return If we can do the aggregation with the frag md or not.
   */
  inline bool can_aggregate_tile_with_frag_md(ResultCellSlab& rcs) {
    auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(rcs.tile_);
    auto& frag_md = fragment_metadata_[rt->frag_idx()];

    // Here we only aggregate a full tile if first of all there are no missing
    // cells in the bitmap. This can be validated with 'copy_full_tile'. Second,
    // we only do it when a full tile is used in the result cell slab structure
    // by making sure that the cell slab starts at 0 and ends at the end of the
    // tile. When we perform the merge to order everything in global order for
    // this reader, we might end up not using a cell in a tile at all because it
    // has a duplicate entry (with the same coordinates) written at a later
    // timestamp. There is no way to know that this happened in a tile at the
    // moment so the best we can do for now is to use fragment metadata only
    // when a full tile was merged in the cell slab structure. Finally, we check
    // the fragment metadata has indeed tile metadata.
    return rt->copy_full_tile() && rcs.start_ == 0 &&
           rcs.length_ == rt->cell_num() && frag_md->has_tile_metadata();
  }

  /**
   * Process aggregates.
   *
   * @param num_range_threads Total number of range threads.
   * @param name Field to aggregate.
   * @param cell_offsets Cell offset per result tile.
   * @param result_cell_slabs The result cell slabs to process.
   */
  void process_aggregates(
      const uint64_t num_range_threads,
      const std::string name,
      std::vector<uint64_t>& cell_offsets,
      std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Remove a result tile from memory
   *
   * @param frag_idx Fragment index.
   * @param rt Iterator to the result tile to remove.
   * @param result_tiles Result tiles per fragment.
   */
  void remove_result_tile(
      const unsigned frag_idx,
      TileListIt rt,
      std::vector<ResultTilesList>& result_tiles);

  /**
   * Clean up processed data after copying and get ready for the next
   * iteration.
   *
   * @param result_tiles Result tiles per fragment.
   */
  void end_iteration(std::vector<ResultTilesList>& result_tiles);
};

}  // namespace tiledb::sm

#endif  // TILEDB_SPARSE_GLOBAL_ORDER_READER
